{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}

-- |
-- Module:      Network.Riak.CRDT.Internal
-- Copyright:   (c) 2016 Sentenai
-- Author:      Antonio Nikishaev <me@lelf.lu>, Mitchell Rosen <mitchellwrosen@gmail.com>
-- Stability:   experimental

module Network.Riak.CRDT.Internal where

import           Control.Applicative
import           Control.Exception
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy.Char8 as Char8
import           Data.Foldable (foldr')
import           Data.Semigroup
import qualified Data.Sequence
import qualified Data.Set
import           Data.Typeable
import qualified Network.Riak.Connection as Conn
import           Network.Riak.Protocol.DtOp (DtOp)
import           Network.Riak.Protocol.DtUpdateRequest (DtUpdateRequest)
import qualified Network.Riak.Protocol.DtUpdateRequest as DtUpdateRequest
import           Network.Riak.Protocol.DtFetchRequest (DtFetchRequest)
import qualified Network.Riak.Protocol.DtFetchRequest as DtFetchRequest
import           Network.Riak.Protocol.DtFetchResponse (DtFetchResponse)
import qualified Network.Riak.Protocol.DtFetchResponse as DtFetchResponse
import qualified Network.Riak.Protocol.DtFetchResponse.DataType as DtFetchResponse
import           Network.Riak.Protocol.DtValue (DtValue)
import           Network.Riak.Types hiding (bucket, key)
import qualified Text.ProtocolBuffers as Proto


class CRDTOp (Op a) => CRDT a where
  data Op a

  modify :: Op a -> a -> a

class Semigroup a => CRDTOp a where
  -- | The protobuf data structure that corresponds to an update of this type of
  -- operation.
  type UpdateOp a

  -- | Marshal a Haskell op to its protobuf form.
  updateOp :: a -> UpdateOp a

  -- | Lift a Haskell op all the way to a protobuf DtOp (the union of all
  -- possible operations). This necessarily is implemented using 'updateOp'.
  unionOp  :: a -> DtOp

type Context = ByteString

data CRDTException
  = CRDTTypeMismatch
      BucketType
      Bucket
      Key
      DtFetchResponse.DataType -- Expected type
      DtFetchResponse.DataType -- Actual type
  -- ^ A fetch was performed for one data type, but another was returned.
  deriving (Show, Typeable)

instance Exception CRDTException where
#if MIN_VERSION_base(4,8,0)
  -- For error-reporting purposes, just snip each byte in the bucket type,
  -- bucket, and key to 8 bits so it's printable-ish.
  displayException = \case
    CRDTTypeMismatch typ bucket key expected actual ->
      "When fetching a data type at " ++ Char8.unpack typ ++ "/" ++
        Char8.unpack bucket ++ "/" ++ Char8.unpack key ++ ", expected a " ++
        showdt expected ++ " but got a " ++ showdt actual
      where
        showdt :: DtFetchResponse.DataType -> String
        showdt = \case
          DtFetchResponse.COUNTER -> "counter"
          DtFetchResponse.MAP     -> "map"
          DtFetchResponse.SET     -> "set"
#endif


-- | Send an update request to Riak. This uses the default
-- 'DtUpdateRequest.DtUpdateRequest' as returned by 'updateRequest':
--
-- @
-- 'sendModify' conn typ bucket key op = 'Conn.exchange_ conn ('updateRequest' typ bucket key op)
-- @
--
-- This is provided for the common case that the @type@, @bucket@, @key@, and
-- @op@ fields are the only ones you wish to set.
--
-- If you want to further modify the update request, simply construct it
-- manually, then send the request using 'Conn.exchange_'. For example,
--
-- @
-- 'Conn.exchange_' conn req
--   where
--     req :: 'DtUpdateRequest.DtUpdateRequest'
--     req = ('updateRequest' typ bucket key op) { 'DtUpdateRequest.timeout' = 'Just' 1000 })
-- @
sendModify
  :: CRDTOp op
  => Connection -> BucketType -> Bucket -> Key -> op -> IO ()
sendModify conn typ bucket key op =
  Conn.exchange_ conn (updateRequest typ bucket key op)

-- | Send an update request to Riak. Like 'sendModify', but also sets @context@.
--
-- This is provided for the common case that the @type@, @bucket@, @key@, @op@,
-- and @context@ fields are the only ones you wish to set.
sendModifyCtx
  :: CRDTOp op
  => Connection -> BucketType -> Bucket -> Key -> Context -> op -> IO ()
sendModifyCtx conn typ bucket key ctx op = Conn.exchange_ conn req
  where
    req :: DtUpdateRequest
    req = (updateRequest typ bucket key op)
            { DtUpdateRequest.context = Just ctx }

-- | A 'DtUpdateRequest.DtUpdateRequest' with the @type@, @bucket@, @key@, and
-- @op@ fields set.
updateRequest
  :: CRDTOp op
  => BucketType -> Bucket -> Key -> op -> DtUpdateRequest
updateRequest typ bucket key op = Proto.defaultValue
  { DtUpdateRequest.type'  = typ
  , DtUpdateRequest.bucket = bucket
  , DtUpdateRequest.key    = Just key
  , DtUpdateRequest.op     = unionOp op
  }


-- | Fetch a data type at the given @bucket@, @type@, and @key@. This uses the
-- default 'DtFetchRequest.DtFetchRequest' as returned by 'fetchRequest':
--
-- @
-- 'fetchRaw' conn typ bucket key = 'Conn.exchange conn ('fetchRequest' typ bucket key)
-- @
--
-- This is provided for the common case that the @type@, @bucket@, and @key@
-- fields are the only ones you wish to set.
--
-- If you want to further modify the update request, simply construct it
-- manually, then send the request using 'Conn.exchange'. For example,
--
-- @
-- 'Conn.exchange' conn req
--   where
--     req :: 'DtFetchRequest.DtFetchRequest'
--     req = ('fetchRequest' typ bucket key) { 'DtFetchRequest.timeout' = 'Just' 1000 })
-- @
--
-- The higher-level @fetch@ functions that pick apart the response should
-- usually suffice; see 'Network.Riak.CRDT.Counter.fetch',
-- 'Network.Riak.CRDT.Map.fetch', and 'Network.Riak.CRDT.Set.fetch'.
fetchRaw
  :: Connection -> BucketType -> Bucket -> Key
  -> IO DtFetchResponse
fetchRaw conn typ bucket key = Conn.exchange conn (fetchRequest typ bucket key)

fetchRequest :: BucketType -> Bucket -> Key -> DtFetchRequest
fetchRequest typ bucket key = Proto.defaultValue
  { DtFetchRequest.type'  = typ
  , DtFetchRequest.bucket = bucket
  , DtFetchRequest.key    = key
  }


-- | An internal function that fetches a specific data type and fails with a
-- 'CRDTTypeMismatch' exception otherwise.
fetchInternal
  :: forall a.
     DtFetchResponse.DataType -- Expected type
  -> (DtValue -> a)           -- Projection from DataType
  -> Connection
  -> DtFetchRequest
  -> IO (Maybe (a, Maybe Context))
fetchInternal expected prj conn req = Conn.exchange conn req >>= go
  where
    go :: DtFetchResponse.DtFetchResponse -> IO (Maybe (a, Maybe Context))
    go resp =
      case DtFetchResponse.type' resp of
        actual | actual == expected -> pure resp'
        actual -> throwIO (CRDTTypeMismatch typ bucket key expected actual)
      where
        typ    = DtFetchRequest.type'  req
        bucket = DtFetchRequest.bucket req
        key    = DtFetchRequest.key    req

        resp' :: Maybe (a, Maybe Context)
        resp' = do
          -- Weird (but possible) to get back a response with Nothing as the
          -- value. This means "not found" (so return Nothing).
          value <- DtFetchResponse.value resp
          pure (prj value, DtFetchResponse.context resp)


--------------------------------------------------------------------------------
-- Misc. functions shared by individual CRDTs, but not exported by
-- Network.Riak.CRDT

seqToSet :: Ord a => Data.Sequence.Seq a -> Data.Set.Set a
seqToSet = foldr' Data.Set.insert mempty
