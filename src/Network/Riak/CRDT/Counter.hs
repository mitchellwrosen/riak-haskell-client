{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE TypeFamilies               #-}

-- |
-- Module:      Network.Riak.CRDT.Internal
-- Copyright:   (c) 2016 Sentenai
-- Author:      Antonio Nikishaev <me@lelf.lu>, Mitchell Rosen <mitchellwrosen@gmail.com>
-- Stability:   experimental

module Network.Riak.CRDT.Counter
  ( Counter
  , Count
  , counterVal
  , incr
  , fetch
  , fetchWith
  ) where

import           Control.Applicative
import           Control.Exception
import           Data.Semigroup
import qualified Network.Riak.Connection                        as Conn
import           Network.Riak.CRDT.Internal
import qualified Network.Riak.Protocol.CounterOp                as CounterOp
import qualified Network.Riak.Protocol.DtOp                     as DtOp
import qualified Network.Riak.Protocol.DtFetchRequest           as DtFetchRequest
import qualified Network.Riak.Protocol.DtFetchResponse          as DtFetchResponse
import qualified Network.Riak.Protocol.DtFetchResponse.DataType as DtFetchResponse
import qualified Network.Riak.Protocol.DtValue                  as DtValue
import           Network.Riak.Types
import qualified Text.ProtocolBuffers                           as Proto

import Data.Int     (Int64)
import GHC.Generics (Generic)


newtype Counter = Counter Count
  deriving (Eq, Ord, Num, Show, Generic)

type Count = Int64

counterVal :: Counter -> Count
counterVal (Counter i) = i


instance CRDT Counter where
  data Op Counter
    = CounterInc !Count
    deriving (Eq, Show)

  modify :: Op Counter -> Counter -> Counter
  modify (CounterInc i) (Counter j) = Counter (i + j)

instance Semigroup (Op Counter) where
  CounterInc i <> CounterInc j = CounterInc (i + j)

instance CRDTOp (Op Counter) where
  type UpdateOp (Op Counter) = CounterOp.CounterOp

  updateOp :: Op Counter -> UpdateOp (Op Counter)
  updateOp (CounterInc i) = CounterOp.CounterOp (Just i)

  unionOp :: Op Counter -> DtOp.DtOp
  unionOp op = Proto.defaultValue { DtOp.counter_op = Just (updateOp op) }


-- | Increment operation.
incr :: Count -> Op Counter
incr = CounterInc


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
fetchWith :: Connection -> DtFetchRequest.DtFetchRequest -> IO (Maybe Counter)
fetchWith conn req = Conn.exchange conn req >>= go
  where
    go :: DtFetchResponse.DtFetchResponse -> IO (Maybe Counter)
    go resp =
      case DtFetchResponse.type' resp of
        DtFetchResponse.COUNTER ->
          pure (do
            val   <- DtFetchResponse.value resp
            count <- DtValue.counter_value val
            pure (Counter count))
        t -> throwIO (CRDTTypeMismatch typ bucket key DtFetchResponse.COUNTER t)
      where
        typ    = DtFetchRequest.type'  req
        bucket = DtFetchRequest.bucket req
        key    = DtFetchRequest.key    req
