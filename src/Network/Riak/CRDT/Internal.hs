{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DefaultSignatures   #-}
{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE KindSignatures      #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}

-- |
-- Module:      Network.Riak.CRDT.Internal
-- Copyright:   (c) 2016 Sentenai
-- Author:      Antonio Nikishaev <me@lelf.lu>, Mitchell Rosen <mitchellwrosen@gmail.com>
-- Stability:   experimental

module Network.Riak.CRDT.Internal where

import           Control.Applicative
import           Control.Exception
import           Data.ByteString.Lazy (ByteString)
import           Data.Foldable (foldr')
import           Data.Semigroup
import qualified Data.Sequence
import qualified Data.Set
import qualified Data.Text.Lazy as LText
import qualified Data.Text.Lazy.Encoding as LText
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


-- | A data type <http://docs.basho.com/riak/kv/2.1.4/developing/data-types/#data-types-and-context context>.
--
-- The following CRDT operations require a 'Context' when making a request:
--
--     (1) Disabling a 'Network.Riak.CRDT.Flag.Flag' within a 'Network.Riak.CRDT.Map.Map'
--     (2) Removing an item from a 'Network.Riak.CRDT.Set.Set' (whether the 'Network.Riak.CRDT.Set.Set' is on its own or in a 'Network.Riak.CRDT.Map.Map')
--     (3) Removing a field from a 'Network.Riak.CRDT.Map.Map'
--
-- As such, 'Op's are tagged with a type-level 'Bool' indicating whether or not
-- a 'Context' is required.
type Context = ByteString


-- TODO: How to get rid of UOp in docs...
class CRDT a where
  -- | "Untyped" op - that is, the associated data family that does not carry
  -- a Bool type index indicating whether its associated request to Riak
  -- requires a context.
  data UOp a

  -- | The protobuf data structure that corresponds to an update of this type of
  -- operation.
  type UpdateOp a

  modifyU :: UOp a -> a -> a

  -- | This is just '<>', but allows us to drop a 'Semigroup' constriant on
  -- 'UOp' that shows up in the haddocks.
  default combineOp :: Semigroup (UOp a) => UOp a -> UOp a -> UOp a
  combineOp :: UOp a -> UOp a -> UOp a
  combineOp = (<>)

  -- | Marshal a Haskell op to its protobuf form.
  updateOp :: UOp a -> UpdateOp a

  -- | Lift a Haskell op all the way to a protobuf DtOp (the union of all
  -- possible operations). This necessarily is implemented using 'updateOp'.
  unionOp :: UOp a -> DtOp

-- | Modify a Haskell data type with an 'Op'.
modify :: CRDT a => Op a c -> a -> a
modify (Op op) = modifyU op


-- | A CRDT operation, tagged with a type-level boolean indicating if a request
-- sent with this 'Op' requires a 'Context'.
--
-- This is enforced by 'sendModify' and 'sendModifyCtx'.
newtype Op a (c :: Bool) = Op (UOp a)

-- | Combine two 'Op's monoidally. @:||@ is an unexported type family version of
-- '||' with the obvious definition:
--
-- @
-- type family (:||) a b where
--   'False' :|| x = x
--   'True   :|| x = 'True
-- @
(><) :: CRDT a => Op a c -> Op a c' -> Op a (c :|| c')
Op x >< Op y = Op (combineOp x y)
infixr 6 ><

type family (:||) a b where
  'False :|| x = x
  'True  :|| x = 'True


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
  displayException = \case
    CRDTTypeMismatch typ bucket key expected actual ->
      "When fetching a data type at " ++ showbs typ ++ "/" ++ showbs bucket ++
        "/" ++ showbs key ++ ", expected a " ++ showdt expected ++
        " but got a " ++ showdt actual
      where
        showdt :: DtFetchResponse.DataType -> String
        showdt = \case
          DtFetchResponse.COUNTER -> "counter"
          DtFetchResponse.MAP     -> "map"
          DtFetchResponse.SET     -> "set"

        showbs :: ByteString -> String
        showbs = LText.unpack . LText.decodeUtf8
#endif


-- | Send an update request to Riak. This uses the default
-- 'DtUpdateRequest.DtUpdateRequest' as returned by 'updateRequest':
--
-- @
-- 'sendModify' conn typ bucket key op = 'Conn.exchange_' conn ('updateRequest' typ bucket key op)
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
--
-- Note that this circumvents the type-safety built into 'Op', namely that
-- requests requiring a 'Context' are tagged with a type-level 'True'.
sendModify
  :: CRDT a
  => Connection -> BucketType -> Bucket -> Key -> Op a 'False -> IO ()
sendModify conn typ bucket key op =
  Conn.exchange_ conn (updateRequest typ bucket key op)

-- | Send an update request to Riak. Like 'sendModify', but also sets @context@.
--
-- This is provided for the common case that the @type@, @bucket@, @key@, @op@,
-- and @context@ fields are the only ones you wish to set.
sendModifyCtx
  :: CRDT a
  => Connection -> BucketType -> Bucket -> Key -> Context -> Op a c -> IO ()
sendModifyCtx conn typ bucket key ctx op = Conn.exchange_ conn req
  where
    req :: DtUpdateRequest
    req = (updateRequest typ bucket key op)
            { DtUpdateRequest.context = Just ctx }

-- | Construct a  'DtUpdateRequest.DtUpdateRequest' with the @type@, @bucket@,
-- @key@, and @op@ fields set.
--
-- Use this when 'sendModify'/'sendModifyCtx' are insufficient.
updateRequest
  :: CRDT a
  => BucketType -> Bucket -> Key -> Op a c -> DtUpdateRequest
updateRequest typ bucket key (Op op) = Proto.defaultValue
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
-- usually suffice; see @Counter.'Network.Riak.CRDT.Counter.fetch'@,
-- @Map.'Network.Riak.CRDT.Map.fetch'@, and @Set.'Network.Riak.CRDT.Set.fetch'@.
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
