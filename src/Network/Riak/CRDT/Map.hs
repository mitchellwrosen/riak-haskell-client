{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeFamilies               #-}

module Network.Riak.CRDT.Map
  ( -- * Map type
    Map
  , Flag(..)
  , Register(..)
    -- * Map operations
  , updateCounter
  , updateFlag
  , updateRegister
  , updateSet
  , removeCounter
  , removeFlag
  , removeMap
  , removeRegister
  , removeSet
    -- * Map fetch
  , fetch
  , fetchWith
  ) where

import           Control.Applicative
import           Control.DeepSeq (NFData)
import           Data.Bool
import           Data.ByteString.Lazy (ByteString)
import           Data.Default.Class
import           Data.Foldable (foldr')
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import qualified Data.Map as Map
import           Data.Semigroup
import           Data.Sequence (Seq, (<|))
import qualified Data.Sequence as Seq
import qualified Data.Set as Set
import           GHC.Generics (Generic)
import           Network.Riak.CRDT.Counter (Counter)
import           Network.Riak.CRDT.Internal
import           Network.Riak.CRDT.Set (Set)
import qualified Network.Riak.Protocol.CounterOp as CounterOp
import qualified Network.Riak.Protocol.DtOp as DtOp
import           Network.Riak.Protocol.DtFetchRequest (DtFetchRequest)
import qualified Network.Riak.Protocol.DtFetchResponse.DataType as DtFetchResponse
import qualified Network.Riak.Protocol.DtValue as DtValue
import           Network.Riak.Protocol.MapEntry (MapEntry)
import           Network.Riak.Protocol.MapField (MapField(MapField))
import           Network.Riak.Protocol.MapField.MapFieldType (MapFieldType)
import qualified Network.Riak.Protocol.MapField.MapFieldType as MapFieldType
import           Network.Riak.Protocol.MapOp (MapOp(MapOp))
import           Network.Riak.Protocol.MapUpdate (MapUpdate(MapUpdate))
import qualified Network.Riak.Protocol.MapUpdate as MapUpdate
import           Network.Riak.Protocol.MapUpdate.FlagOp (FlagOp)
import qualified Network.Riak.Protocol.MapUpdate.FlagOp as FlagOp
import qualified Network.Riak.Protocol.SetOp as SetOp
import           Network.Riak.Types
import qualified Text.ProtocolBuffers as Proto


data Map = Map
  { counters  :: Map.Map ByteString Counter
  , flags     :: Map.Map ByteString Flag
  , maps      :: Map.Map ByteString Map
  , registers :: Map.Map ByteString Register
  , sets      :: Map.Map ByteString Set
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

instance CRDT Map where
  data Op Map
    = MapMod
        { upd_counters  :: Map.Map ByteString (Op Counter)
        , upd_flags     :: Map.Map ByteString Flag
        , upd_maps      :: Map.Map ByteString (Op Map)
        , upd_registers :: Map.Map ByteString Register
        , upd_sets      :: Map.Map ByteString (Op Set)
        , rem_counters  :: Set.Set ByteString
        , rem_flags     :: Set.Set ByteString
        , rem_maps      :: Set.Set ByteString
        , rem_registers :: Set.Set ByteString
        , rem_sets      :: Set.Set ByteString
        }

  modify :: Op Map -> Map -> Map
  modify MapMod{..} Map{..} = Map
    { counters  = counters'
    , flags     = flags'
    , maps      = maps'
    , registers = registers'
    , sets      = sets'
    }
    where
      counters' :: Map.Map ByteString Counter
      counters' = applyOps upd_counters counters \\ rem_counters

      -- Relies on left-bias of map union
      flags' :: Map.Map ByteString Flag
      flags' = Map.union upd_flags flags \\ rem_flags

      maps' :: Map.Map ByteString Map
      maps' = applyOps upd_maps maps \\ rem_maps

      -- Relies on left-bias of map union
      registers' :: Map.Map ByteString Register
      registers' = Map.union upd_registers registers \\ rem_registers

      sets' :: Map.Map ByteString Set
      sets' = applyOps upd_sets sets \\ rem_sets

      applyOps
        :: forall k a.
           (Ord k, Default a, CRDT a)
        => Map.Map k (Op a) -> Map.Map k a -> Map.Map k a
      applyOps = Map.mergeWithKey apply (fmap applyDef) id
        where
          -- | Apply an operation to a CRDT. Suitable as the first argument of
          -- 'Map.mergeWithKey' (ignores ByteString key, and always returns
          -- Just, to keep the element in the map).
          apply :: k -> Op a -> a -> Maybe a
          apply _ op x = Just (modify op x)

          -- | Apply an operation to the default element of a CRDT. Suitable as
          -- the second argument of 'Map.mergeWithKey' (operations on
          -- non-existent elements cause them to be created).
          applyDef :: Op a -> a
          applyDef op = modify op def

      -- | Remove all of the given keys from a map.
      (\\) :: Ord k => Map.Map k v -> Set.Set k -> Map.Map k v
      m \\ ks = Map.filterWithKey (\k _ -> k `Set.notMember` ks) m

instance Semigroup (Op Map) where
  MapMod a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 <>
    MapMod b0 b1 b2 b3 b4 b5 b6 b7 b8 b9 =
      MapMod (a0 <> b0) (a1 <> b1) (a2 <> b2) (a3 <> b3) (a4 <> b4) (a5 <> b5)
             (a6 <> b6) (a7 <> b7) (a8 <> b8) (a9 <> b9)

instance Monoid (Op Map) where
  mempty = MapMod mempty mempty mempty mempty mempty mempty mempty mempty mempty mempty
  mappend = (<>)

instance CRDTOp (Op Map) where
  type UpdateOp (Op Map) = MapOp

  updateOp :: Op Map -> UpdateOp (Op Map)
  updateOp MapMod{..} = MapOp removes updates
    where
      removes :: Seq MapField
      removes =
        toRemoves MapFieldType.COUNTER  rem_counters  <>
        toRemoves MapFieldType.FLAG     rem_flags     <>
        toRemoves MapFieldType.MAP      rem_maps      <>
        toRemoves MapFieldType.REGISTER rem_registers <>
        toRemoves MapFieldType.SET      rem_sets
        where
          toRemoves :: MapFieldType -> Set.Set ByteString -> Seq MapField
          toRemoves typ =
            foldr' (\name s -> MapField name typ <| s) mempty

      updates :: Seq MapUpdate.MapUpdate
      updates =
        foldMapToSeq counterUpdate  upd_counters  <>
        foldMapToSeq flagUpdate     upd_flags     <>
        foldMapToSeq mapUpdate      upd_maps      <>
        foldMapToSeq registerUpdate upd_registers <>
        foldMapToSeq setUpdate      upd_sets
        where
          foldMapToSeq :: (k -> v -> a) -> Map.Map k v -> Seq a
          foldMapToSeq f = Map.foldMapWithKey (\k v -> Seq.singleton (f k v))

          counterUpdate :: ByteString -> Op Counter -> MapUpdate
          counterUpdate name op = Proto.defaultValue
            { MapUpdate.field = MapField name MapFieldType.COUNTER
            , MapUpdate.counter_op = Just (updateOp op)
            }

          flagUpdate :: ByteString -> Flag -> MapUpdate
          flagUpdate name flag = Proto.defaultValue
            { MapUpdate.field = MapField name MapFieldType.FLAG
            , MapUpdate.flag_op =
                Just (if flagVal flag then FlagOp.ENABLE else FlagOp.DISABLE)
            }

          mapUpdate :: ByteString -> Op Map -> MapUpdate
          mapUpdate name op = Proto.defaultValue
            { MapUpdate.field = MapField name MapFieldType.MAP
            , MapUpdate.map_op = Just (updateOp op)
            }

          registerUpdate :: ByteString -> Register -> MapUpdate
          registerUpdate name reg = Proto.defaultValue
            { MapUpdate.field = MapField name MapFieldType.REGISTER
            , MapUpdate.register_op = Just (registerVal reg)
            }

          setUpdate :: ByteString -> Op Set -> MapUpdate
          setUpdate name op = Proto.defaultValue
            { MapUpdate.field = MapField name MapFieldType.SET
            , MapUpdate.set_op = Just (updateOp op)
            }

  unionOp :: Op Map -> DtOp.DtOp
  unionOp = undefined


newtype Flag
  = Flag { flagVal :: Bool }
  deriving (Eq, Show, Generic)

instance NFData Flag


newtype Register
  = Register { registerVal :: ByteString }
  deriving (Eq, Show, Generic)

instance NFData Register


-- | Update 'Counter' operation.
updateCounter :: NonEmpty ByteString -> Op Counter -> Op Map
updateCounter names op =
  updateMapWith names (\name -> mempty { upd_counters = Map.singleton name op })

-- | Update 'Flag' operation.
updateFlag :: NonEmpty ByteString -> Flag -> Op Map
updateFlag names flag =
  updateMapWith names (\name -> mempty { upd_flags = Map.singleton name flag })

-- | Update 'Register' operation.
updateRegister :: NonEmpty ByteString -> Register -> Op Map
updateRegister names reg =
  updateMapWith names (\name -> mempty { upd_registers = Map.singleton name reg })

-- | Update 'Set' operation.
updateSet :: NonEmpty ByteString -> Op Set -> Op Map
updateSet names op =
  updateMapWith names (\name -> mempty { upd_sets = Map.singleton name op })

-- | Internal update 'Map' operation. Not exported, because the only way to
-- update a 'Map' is by updating a 'Counter', 'Flag', 'Register', or 'Set'
-- inside of it.
updateMapWith :: NonEmpty ByteString -> (ByteString -> Op Map) -> Op Map
updateMapWith xs0 f = go (NonEmpty.toList xs0)
  where
    go :: [ByteString] -> Op Map
    go [x]    = f x
    go (x:xs) = mempty { upd_maps = Map.singleton x (go xs) }
    go _      = error "Network.Riak.CRDT.Map.updateMapWith: empty list"

-- | Remove 'Counter' operation.
removeCounter :: NonEmpty ByteString -> Op Map
removeCounter = removeFromMap (\x -> mempty { rem_counters = x })

-- | Remove 'Flag' operation.
removeFlag :: NonEmpty ByteString -> Op Map
removeFlag = removeFromMap (\x -> mempty { rem_flags = x })

-- | Remove 'Map' operation.
removeMap :: NonEmpty ByteString -> Op Map
removeMap = removeFromMap (\x -> mempty { rem_maps = x })

-- | Remove 'Register' operation.
removeRegister :: NonEmpty ByteString -> Op Map
removeRegister = removeFromMap (\x -> mempty { rem_registers = x })

-- | Remove 'Set' operation.
removeSet :: NonEmpty ByteString -> Op Map
removeSet = removeFromMap (\x -> mempty { rem_sets = x })

removeFromMap :: (Set.Set ByteString -> Op Map) -> NonEmpty ByteString -> Op Map
removeFromMap f = go . NonEmpty.toList
  where
    go :: [ByteString] -> Op Map
    go [x]    = f (Set.singleton x)
    go (x:xs) = mempty { upd_maps = Map.singleton x (go xs) }
    go _      = error "Network.Riak.CRDT.Map.removeFromMap: empty list"


-- | Fetch a 'Map'. This uses the default 'DtFetchRequest.DtFetchRequest' as
-- returned by 'fetchRequest':
--
-- @
-- 'fetch' conn typ bucket key = 'fetchWith' conn ('fetchRequest' typ bucket key)
-- @
--
-- Throws 'CRDTTypeMismatch' if the given bucket type, bucket, and key does not
-- contain a 'Map'.
fetch :: Connection -> BucketType -> Bucket -> Key -> IO (Maybe (Map, Context))
fetch conn typ bucket key =
  fmap go <$> fetchWith conn (fetchRequest typ bucket key)
  where
    go :: (Map, Maybe Context) -> (Map, Context)
    go (m, Just c) = (m, c)
    -- @include_context@ was not included in request, so it should default to
    -- true. Therefore, we should always get a context back.
    go _ = error "impossible"

-- | Fetch a 'Map' with the given 'DtFetchRequest.DtFetchRequest'.
--
-- Throws 'CRDTTypeMismatch' if the given bucket type, bucket, and key does
-- not contain a 'Map'.
fetchWith :: Connection -> DtFetchRequest -> IO (Maybe (Map, Maybe Context))
fetchWith conn req =
  fmap go <$> fetchInternal DtFetchResponse.MAP DtValue.map_value conn req
  where
    go :: (Seq MapEntry, Maybe Context) -> (Map, Maybe Context)
    go (m, ctx) = (m', ctx)
      where
        m' :: Map
        m' = undefined -- TODO
