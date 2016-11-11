{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}

-- |
-- Module:     Network.Riak.CRDT.Counter
-- Copyright:  (c) 2016 Sentenai
-- Maintainer: Antonio Nikishaev <me@lelf.lu>, Mitchell Rosen <mitchellwrosen@gmail.com>
-- Stability:  experimental

module Network.Riak.CRDT.Counter
  ( -- * Counter type
    Counter(..)
  , Count
    -- * Counter operations
  , incr
    -- * Counter fetch
  , fetch
  , fetchWith
  ) where

import           Control.Applicative
import           Control.DeepSeq (NFData)
import           Data.Default.Class
import           Data.Int (Int64)
import           Data.Semigroup
import           Network.Riak.CRDT.Internal
import qualified Network.Riak.Protocol.CounterOp as Proto.CounterOp
import qualified Network.Riak.Protocol.DtOp as Proto.DtOp
-- import           Network.Riak.Protocol.DtFetchRequest (DtFetchRequest)
-- import qualified Network.Riak.Protocol.DtFetchResponse.DataType as DtFetchResponse
-- import qualified Network.Riak.Protocol.DtValue as DtValue
import           Network.Riak.Types hiding (bucket, key)
import qualified Text.ProtocolBuffers as Proto


-- instance CRDT Counter where
--   data UOp Counter
--     = CounterInc !Count
--     deriving (Eq, Show)
--
--   -- type UpdateOp Counter = CounterOp
--
--   _modifyU :: UOp Counter -> Counter -> Counter
--   _modifyU (CounterInc i) (Counter j) = Counter (i + j)
--
--   _updateOp :: UOp Counter -> UpdateOp Counter
--   _updateOp (CounterInc i) = CounterOp (Just i)
--
--   _unionOp :: UOp Counter -> DtOp
--   _unionOp op = Proto.defaultValue { DtOp.counter_op = Just (_updateOp op) }

newtype CounterOp = Increment Int64

instance Semigroup CounterOp where
  Increment i <> Increment j = Increment (i + j)

-- | Increment operation.
--
-- Example:
--
-- @
-- 'sendModify' conn "foo" "bar" "baz" ('incr' 1)
-- @
incr :: Count -> CounterOp
incr = Increment

data ResponseBody
  = DontReturnBody
  | ReturnBody

data SResponseBody :: ResponseBody -> * where
  SDontReturnBody :: SResponseBody DontReturnBody
  SReturnBody     :: SResponseBody ReturnBody

data UpdateRequest (key :: RequestKey) (body :: ResponseBody)
  = UpdateRequest DtUpdateRequest (SRequestKey key) (SResponseBody body)

updateRequest
  :: BucketType -> Bucket -> CounterOp
  -> UpdateRequest AssignedKey DontReturnBody
updateRequest type' bucket (Increment n) =
  UpdateRequest req SAssignedKey SDontReturnBody
  where
    req :: Proto.DtUpdateRequest.DtUpdateRequest
    req = Proto.defaultValue
      { Proto.DtUpdateRequest.type' = type'
      , Proto.DtUpdateRequest.bucket = bucket
      , Proto.DtUpdateRequest.op = Proto.defaultValue
        { Proto.DtOp.counter_op = Just (Proto.CounterOp.CounterOp (Just n)) }
      }

setKey
  :: Key -> UpdateRequest AssignedKey body -> UpdateRequest ProvidedKey body
setKey key (UpdateRequest req _ body) = UpdateRequest req' SProvidedKey body
  where
    req' :: Proto.DtUpdateRequest.DtUpdateRequest
    req' = req { Proto.DtUpdateRequest.key = Just key }

setReturnBody
  :: UpdateRequest key DontReturnBody -> UpdateRequest key ReturnBody
setReturnBody (UpdateRequest req key _) = UpdateRequest req' key SReturnBody
  where
    req' :: Proto.DtUpdateRequest.DtUpdateRequest
    req' = req { Proto.DtUpdateRequest.return_body = Just True }

type family UpdateResponse (key :: RequestKey) (body :: ResponseBody) where
  UpdateResponse AssignedKey DontReturnBody = Key
  UpdateResponse AssignedKey ReturnBody     = (Key, Counter)
  UpdateResponse ProvidedKey DontReturnBody = ()
  UpdateResponse ProvidedKey ReturnBody     = Counter

update :: Connection -> UpdateRequest key body -> IO (UpdateResponse key body)
update conn (UpdateRequest s_key s_body) = do
  resp <- Conn.exchange conn req

  let key :: Key
      key =
        fromMaybe (error "Network.Riak.CRDT.Counter.update: no key")
          (Proto.DtUpdateResponse.key resp)

      value :: Counter
      value =
        maybe (error "Network.Riak.CRDT.Counter.update: no counter_value")
          Counter (DtUpdateResponse.counter_value resp)

  case s_key of
    AssignedKey ->
      case s_body of
        DontReturnBody -> pure key
        ReturnBody     -> pure (key, value)
    ProvidedKey ->
      case s_body of
        DontReturnBody -> pure ()
        ReturnBody     -> pure value

localUpdate :: CounterOp -> Counter -> Counter
localUpdate = undefined

-- | Fetch a 'Counter'. This uses the default 'DtFetchRequest.DtFetchRequest' as
-- returned by 'fetchRequest':
--
-- @
-- 'fetch' conn typ bucket key = 'fetchWith' conn ('fetchRequest' typ bucket key)
-- @
--
-- Throws 'CRDTTypeMismatch' if the given bucket type, bucket, and key does not
-- contain a 'Counter'.
fetch :: Connection -> BucketType -> Bucket -> Key -> IO (Maybe Counter)
fetch conn typ bucket key = fetchWith conn (fetchRequest typ bucket key)

-- | Fetch a 'Counter' with the given 'DtFetchRequest.DtFetchRequest'.
--
-- Throws 'CRDTTypeMismatch' if the given bucket type, bucket, and key does
-- not contain a 'Counter'.
fetchWith :: Connection -> DtFetchRequest -> IO (Maybe Counter)
fetchWith conn req =
  fmap go <$>
    fetchInternal DtFetchResponse.COUNTER DtValue.counter_value conn req
  where
    go :: (Maybe Int64, Maybe (Context a)) -> Counter
    go (Just i, _) = Counter i
    -- type = COUNTER but counter_val = Nothing? Riak will never do this
    go _ = error "Network.Riak.CRDT.Counter.fetchWith: Nothing"
