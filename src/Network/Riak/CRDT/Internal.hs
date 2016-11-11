{-# LANGUAGE CPP                        #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DefaultSignatures          #-}
{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures             #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE PolyKinds                  #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}

-- |
-- Module:     Network.Riak.CRDT.Internal
-- Copyright:  (c) 2016 Sentenai
-- Maintainer: Antonio Nikishaev <me@lelf.lu>, Mitchell Rosen <mitchellwrosen@gmail.com>
-- Stability:  experimental

module Network.Riak.CRDT.Internal where

import           Control.Applicative
import           Control.DeepSeq (NFData)
import           Control.Exception
import           Data.ByteString.Lazy (ByteString)
import           Data.Default.Class
import           Data.Foldable (foldr')
import           Data.Int (Int64)
import qualified Data.Map
import           Data.Maybe (fromMaybe)
import           Data.Semigroup
import qualified Data.Sequence
import qualified Data.Set
import qualified Data.Text.Lazy as LText
import qualified Data.Text.Lazy.Encoding as LText
import           Data.Typeable
import           GHC.Generics (Generic)
import qualified Network.Riak.Connection as Conn
import           Network.Riak.Protocol.DtOp (DtOp)
import           Network.Riak.Protocol.DtUpdateRequest (DtUpdateRequest)
import qualified Network.Riak.Protocol.DtUpdateRequest as DtUpdateRequest
import qualified Network.Riak.Protocol.DtUpdateResponse as DtUpdateResponse
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
-- a 'Context' is required when making an 'UpdateRequest'
newtype Context crdt
  = Context ByteString
  deriving (Eq, Ord, Show)


-- | An opaque CRDT typeclass.
class CRDT a where
  -- | "Untyped" op - that is, the associated data family that does not carry
  -- a Bool type index indicating whether its associated request to Riak
  -- requires a context.
  data UOp a

  -- | The protobuf data structure that corresponds to an update of this type of
  -- operation.
  type UpdateOp a

  _modifyU :: UOp a -> a -> a

  -- | Marshal a Haskell op to its protobuf form.
  _updateOp :: UOp a -> UpdateOp a

  -- | Lift a Haskell op all the way to a protobuf DtOp (the union of all
  -- possible operations). This necessarily is implemented using '_updateOp'.
  _unionOp :: UOp a -> DtOp

  -- | This is just '<>', but allows us to drop a 'Semigroup' constriant on
  -- 'UOp' that shows up in the haddocks.
  default _combineOp :: Semigroup (UOp a) => UOp a -> UOp a -> UOp a
  _combineOp :: UOp a -> UOp a -> UOp a
  _combineOp = (<>)

  _singCrdt :: Sing a


-- |
-- @
-- 'Op' crdt ctx
--    |    |
--    |    \''True' or \''False'
--    |
--    'Network.Riak.CRDT.Counter.Counter', 'Network.Riak.CRDT.Map.Map', or 'Network.Riak.CRDT.Set.Set'
-- @
--
-- A 'CRDT' operation, tagged with a type-level boolean indicating if an
-- 'UpdateRequest' sent with this 'Op' requires a 'Context'.
newtype Op crdt (ctx :: Bool) = Op (UOp crdt)

type family (:||) a b where
  'False :|| x = x
  'True  :|| x = 'True

-- | Combine two 'Op's monoidally. @:||@ is an unexported type family version of
-- '||' with the obvious definition:
--
-- @
-- type family (:||) a b where
--   'False :|| x = x
--   'True  :|| x = 'True
-- @
(><) :: CRDT crdt => Op crdt ctx -> Op crdt ctx' -> Op crdt (ctx :|| ctx')
Op x >< Op y = Op (_combineOp x y)
infixr 6 ><


data CRDTException
  = CRDTTypeMismatch BucketType Bucket Key DtFetchResponse.DataType
      DtFetchResponse.DataType
  -- ^ A fetch was performed for the first 'DataType', but the second one was
  -- returned instead.
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


data RequestKey
  = AssignedKey
  | ProvidedKey

data SRequestKey :: RequestKey -> * where
  SAssignedKey :: SRequestKey AssignedKey
  SProvidedKey :: SRequestKey ProvidedKey

-- data ResponseBody
--   = DontReturnBody
--   | ReturnBody
--   | ReturnContext

data UpdateRequest
  (crdt :: *)
  (ctx :: Bool)
  (key :: RequestKey)
  (body :: ResponseBody)
    = UpdateRequest DtUpdateRequest (Sing crdt) (Sing key) (Sing body)

updateRequest_
  :: CRDT crdt
  => BucketType
  -> Bucket
  -> Op crdt ctx
  -> UpdateRequest crdt ctx AssignedKey DontReturnBody
updateRequest_ type' bucket (Op op) =
  UpdateRequest req _singCrdt SAssignedKey SDontReturnBody
  where
    req :: DtUpdateRequest
    req = Proto.defaultValue
      { DtUpdateRequest.type'  = type'
      , DtUpdateRequest.bucket = bucket
      , DtUpdateRequest.op     = _unionOp op
      }

setKey
  :: Key
  -> UpdateRequest crdt ctx AssignedKey body
  -> UpdateRequest crdt ctx ProvidedKey body
setKey key (UpdateRequest req crdt _ body) =
  UpdateRequest req' crdt SProvidedKey body
  where
    req' :: DtUpdateRequest
    req' = req { DtUpdateRequest.key = Just key }

setContext
  :: Context crdt
  -> UpdateRequest crdt False key body
  -> UpdateRequest crdt True  key body
setContext (Context ctx) (UpdateRequest req crdt key body) =
  UpdateRequest (req { DtUpdateRequest.context = Just ctx }) crdt key body

setReturnBody
  :: UpdateRequest crdt ctx key DontReturnBody
  -> UpdateRequest crdt ctx key ReturnBody
setReturnBody (UpdateRequest req crdt key _) =
  UpdateRequest req' crdt key SReturnBody
  where
    req' :: DtUpdateRequest
    req' = req { DtUpdateRequest.return_body = Just True }

setReturnContext
  :: UpdateRequest crdt ctx key DontReturnBody
  -> UpdateRequest crdt ctx key ReturnContext
setReturnContext (UpdateRequest req crdt key _) =
  UpdateRequest req' crdt key SReturnContext
  where
    req' :: DtUpdateRequest
    req' = req
      { DtUpdateRequest.return_body     = Just True
      , DtUpdateRequest.include_context = Just True
      }

type family UpdateResponse (crdt :: *) (key :: RequestKey) (body :: ResponseBody) where
  UpdateResponse crdt ProvidedKey DontReturnBody = ()
  UpdateResponse crdt ProvidedKey ReturnBody     = crdt
  UpdateResponse crdt ProvidedKey ReturnContext  = (crdt, Context crdt)
  UpdateResponse crdt AssignedKey DontReturnBody = Key
  UpdateResponse crdt AssignedKey ReturnBody     = (Key, crdt)
  UpdateResponse crdt AssignedKey ReturnContext  = (Key, crdt, Context crdt)

sendUpdateRequest
  :: forall crdt ctx key body.
     Connection
  -> UpdateRequest crdt ctx key body
  -> IO (UpdateResponse crdt key body)
sendUpdateRequest conn (UpdateRequest req s_crdt s_key s_body) = do
  resp <- Conn.exchange conn req

  let counterVal :: Counter
      counterVal =
        maybe (error "Network.Riak.CRDT.Internal.sendUpdateRequest: no counter_value")
          Counter (DtUpdateResponse.counter_value resp)

      setVal :: Set
      setVal = Set (seqToSet (DtUpdateResponse.set_value resp))

      mapVal :: Map
      mapVal = undefined

      key :: Key
      key =
        fromMaybe (error "Network.Riak.CRDT.Internal.sendUpdateRequest: no key")
          (DtUpdateResponse.key resp)

      context :: Context crdt
      context =
        maybe (error "Network.Riak.CRDT.Internal.sendUpdateRequest: no context")
          Context (DtUpdateResponse.context resp)

  case s_crdt of
    SCounter ->
      case s_key of
        SProvidedKey ->
          case s_body of
            SDontReturnBody -> pure ()
            SReturnBody     -> pure counterVal
            SReturnContext  -> pure (counterVal, Context mempty) -- FIXME
        SAssignedKey ->
          case s_body of
            SDontReturnBody -> pure key
            SReturnBody     -> pure (key, counterVal)
            SReturnContext  -> pure (key, counterVal, Context mempty) -- FIXME
    SSet ->
      case s_key of
        SProvidedKey ->
          case s_body of
            SDontReturnBody -> pure ()
            SReturnBody     -> pure setVal
            SReturnContext  -> pure (setVal, context)
        SAssignedKey ->
          case s_body of
            SDontReturnBody -> pure key
            SReturnBody     -> pure (key, setVal)
            SReturnContext  -> pure (key, setVal, context)
    SMap ->
      case s_key of
        SProvidedKey ->
          case s_body of
            SDontReturnBody -> pure ()
            SReturnBody     -> pure mapVal
            SReturnContext  -> pure (mapVal, context)
        SAssignedKey ->
          case s_body of
            SDontReturnBody -> pure key
            SReturnBody     -> pure (key, mapVal)
            SReturnContext  -> pure (key, mapVal, context)


data family Sing (a :: k)

data instance Sing (a :: *) where
  SCounter :: Sing Counter
  SSet     :: Sing Set
  SMap     :: Sing Map

-- data instance Sing (a :: ResponseBody) where
--   SDontReturnBody :: Sing DontReturnBody
--   SReturnBody     :: Sing ReturnBody
--   SReturnContext  :: Sing ReturnContext

-- | Modify a 'CRDT' locally with its associated 'Op'.
--
-- @
-- >>> let c = 'Network.Riak.Counter.Counter' 0
-- >>> 'modify' ('Network.Riak.Counter.incr' 1 '><' 'Network.Riak.Counter.incr' 2) c
-- Counter 3
-- @
modify :: CRDT a => Op a c -> a -> a
modify (Op op) = _modifyU op


-- -- | Send an update request to Riak. This uses the default
-- -- 'DtUpdateRequest.DtUpdateRequest' as returned by 'updateRequest':
-- --
-- -- @
-- -- 'sendModify' conn typ bucket key op = 'Conn.exchange_' conn ('updateRequest' typ bucket key op)
-- -- @
-- --
-- -- This is provided for the common case that the @type@, @bucket@, @key@, and
-- -- @op@ fields are the only ones you wish to set.
-- --
-- -- If you want to further modify the update request, simply construct it
-- -- manually, then send the request using 'Conn.exchange_' as above. For example,
-- --
-- -- @
-- -- 'Conn.exchange_' conn req
-- --   where
-- --     req :: 'DtUpdateRequest.DtUpdateRequest'
-- --     req = ('updateRequest' typ bucket key op) { 'DtUpdateRequest.timeout' = 'Just' 1000 }
-- -- @
-- --
-- -- Note that this circumvents the type-safety built into 'Op', namely that
-- -- requests requiring a 'Context' are tagged with a type-level 'True'.
-- sendModify
--   :: CRDT a
--   => Connection -> BucketType -> Bucket -> Key -> Op a 'False -> IO ()
-- sendModify conn typ bucket key op =
--   Conn.exchange_ conn (updateRequest typ bucket key op)
--
-- -- | Send an update request to Riak. Like 'sendModify', but also sets @context@.
-- --
-- -- This is provided for the common case that the @type@, @bucket@, @key@, @op@,
-- -- and @context@ fields are the only ones you wish to set.
-- sendModifyCtx
--   :: CRDT a
--   => Connection -> BucketType -> Bucket -> Key -> Context -> Op a c -> IO ()
-- sendModifyCtx conn typ bucket key ctx op = Conn.exchange_ conn req
--   where
--     req :: DtUpdateRequest
--     req = (updateRequest typ bucket key op)
--             { DtUpdateRequest.context = Just ctx }

-- | Construct a  'DtUpdateRequest.DtUpdateRequest' with the @type@, @bucket@,
-- @key@, and @op@ fields set.
--
-- Use this when 'sendModify' or 'sendModifyCtx' are insufficient.
updateRequest
  :: CRDT a
  => BucketType -> Bucket -> Key -> Op a c -> DtUpdateRequest
updateRequest typ bucket key (Op op) = Proto.defaultValue
  { DtUpdateRequest.type'  = typ
  , DtUpdateRequest.bucket = bucket
  , DtUpdateRequest.key    = Just key
  , DtUpdateRequest.op     = _unionOp op
  }


-- | Fetch a data type at the given @bucket@, @type@, and @key@. This uses the
-- default 'DtFetchRequest.DtFetchRequest' as returned by 'fetchRequest':
--
-- @
-- 'fetchRaw' conn typ bucket key = 'Conn.exchange' conn ('fetchRequest' typ bucket key)
-- @
--
-- This is provided for the common case that the @type@, @bucket@, and @key@
-- fields are the only ones you wish to set.
--
-- If you want to further modify the update request, simply construct it
-- manually, then send the request using 'Conn.exchange' as above. For example,
--
-- @
-- 'Conn.exchange' conn req
--   where
--     req :: 'DtFetchRequest.DtFetchRequest'
--     req = ('fetchRequest' typ bucket key) { 'DtFetchRequest.timeout' = 'Just' 1000 }
-- @
--
-- The higher-level @fetch@ functions that parse the response should usually
-- suffice; see @Counter.'Network.Riak.CRDT.Counter.fetch'@,
-- @Map.'Network.Riak.CRDT.Map.fetch'@, and @Set.'Network.Riak.CRDT.Set.fetch'@.
fetchRaw :: Connection -> BucketType -> Bucket -> Key -> IO DtFetchResponse
fetchRaw conn typ bucket key = Conn.exchange conn (fetchRequest typ bucket key)

-- | Construct a 'DtFetchRequest' with the @type@, @bucket@, and @key@ fields
-- set.
--
-- Use this for @Counter.'Network.Riak.CRDT.Counter.fetchWith'@,
-- @Map.'Network.Riak.CRDT.Map.fetchWith'@, and
-- @Set.'Network.Riak.CRDT.Set.fetchWith'@.
fetchRequest :: BucketType -> Bucket -> Key -> DtFetchRequest
fetchRequest typ bucket key = Proto.defaultValue
  { DtFetchRequest.type'  = typ
  , DtFetchRequest.bucket = bucket
  , DtFetchRequest.key    = key
  }


-- | An internal function that fetches a specific data type and fails with a
-- 'CRDTTypeMismatch' exception otherwise.
fetchInternal
  :: forall a crdt.
     DtFetchResponse.DataType -- Expected type
  -> (DtValue -> a)           -- Projection from DataType
  -> Connection
  -> DtFetchRequest
  -> IO (Maybe (a, Maybe (Context crdt)))
fetchInternal expected prj conn req = Conn.exchange conn req >>= go
  where
    go :: DtFetchResponse.DtFetchResponse
       -> IO (Maybe (a, Maybe (Context crdt)))
    go resp =
      case DtFetchResponse.type' resp of
        actual | actual == expected -> pure resp'
        actual -> throwIO (CRDTTypeMismatch typ bucket key expected actual)
      where
        typ    = DtFetchRequest.type'  req
        bucket = DtFetchRequest.bucket req
        key    = DtFetchRequest.key    req

        resp' :: Maybe (a, Maybe (Context crdt))
        resp' = do
          -- Weird (but possible) to get back a response with Nothing as the
          -- value. This means "not found" (so return Nothing).
          value <- DtFetchResponse.value resp
          pure (prj value, Context <$> DtFetchResponse.context resp)


--------------------------------------------------------------------------------
-- Misc. functions shared by individual CRDTs, but not exported by
-- Network.Riak.CRDT

seqToSet :: Ord a => Data.Sequence.Seq a -> Data.Set.Set a
seqToSet = foldr' Data.Set.insert mempty

--------------------------------------------------------------------------------

newtype Counter = Counter { val :: Count }
  deriving (Eq, Ord, Num, Show, Generic)

type Count = Int64

instance NFData Counter

instance Default Counter where
  def = mempty

instance Semigroup Counter where
  Counter i <> Counter j = Counter (i + j)

instance Monoid Counter where
  mempty = Counter 0
  mappend = (<>)


newtype Set = Set (Data.Set.Set ByteString)
  deriving (Eq, Ord, Show, Generic, Monoid, Semigroup)

instance NFData Set

instance Default Set where
  def = mempty


data Map = Map
  { counters  :: Data.Map.Map ByteString Counter
  , flags     :: Data.Map.Map ByteString Flag
  , maps      :: Data.Map.Map ByteString Map
  , registers :: Data.Map.Map ByteString Register
  , sets      :: Data.Map.Map ByteString Set
  } deriving (Eq, Show, Generic)

instance NFData Map

instance Default Map where
  def = mempty

instance Semigroup Map where
  Map a0 a1 a2 a3 a4 <> Map b0 b1 b2 b3 b4 =
    Map (a0 <> b0) (a1 <> b1) (a2 <> b2) (a3 <> b3) (a4 <> b4)

instance Monoid Map where
  mempty = Map mempty mempty mempty mempty mempty
  mappend = (<>)


newtype Flag
  = Flag { flagVal :: Bool }
  deriving (Eq, Show, Generic)

instance NFData Flag


newtype Register
  = Register { registerVal :: ByteString }
  deriving (Eq, Show, Generic)

instance NFData Register
