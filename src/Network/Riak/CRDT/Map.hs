{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE RankNTypes                 #-}
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
  , enableFlag
  , disableFlag
  , setRegister
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
import           Network.Riak.CRDT.Counter (Counter(Counter))
import           Network.Riak.CRDT.Internal
import           Network.Riak.CRDT.Set (Set(Set))
import           Network.Riak.Lens
import qualified Network.Riak.Protocol.CounterOp as CounterOp
import qualified Network.Riak.Protocol.DtOp as DtOp
import           Network.Riak.Protocol.DtFetchRequest (DtFetchRequest)
import qualified Network.Riak.Protocol.DtFetchResponse.DataType as DtFetchResponse
import qualified Network.Riak.Protocol.DtValue as DtValue
import           Network.Riak.Protocol.MapEntry (MapEntry)
import qualified Network.Riak.Protocol.MapEntry as MapEntry
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

countersL :: Lens' Map (Map.Map ByteString Counter)
countersL = lens counters (\m x -> m { counters = x })

flagsL :: Lens' Map (Map.Map ByteString Flag)
flagsL = lens flags (\m x -> m { flags = x })

mapsL :: Lens' Map (Map.Map ByteString Map)
mapsL = lens maps (\m x -> m { maps = x })

registersL :: Lens' Map (Map.Map ByteString Register)
registersL = lens registers (\m x -> m { registers = x })

setsL :: Lens' Map (Map.Map ByteString Set)
setsL = lens sets (\m x -> m { sets = x })

upd_countersL :: Lens' (Op Map) (Map.Map ByteString (Op Counter))
upd_countersL = lens upd_counters (\m x -> m { upd_counters = x })

upd_flagsL :: Lens' (Op Map) (Map.Map ByteString Flag)
upd_flagsL = lens upd_flags (\m x -> m { upd_flags = x })

upd_mapsL :: Lens' (Op Map) (Map.Map ByteString (Op Map))
upd_mapsL = lens upd_maps (\m x -> m { upd_maps = x })

upd_registersL :: Lens' (Op Map) (Map.Map ByteString Register)
upd_registersL = lens upd_registers (\m x -> m { upd_registers = x })

upd_setsL :: Lens' (Op Map) (Map.Map ByteString (Op Set))
upd_setsL = lens upd_sets (\m x -> m { upd_sets = x })

rem_countersL :: Lens' (Op Map) (Set.Set ByteString)
rem_countersL = lens rem_counters (\m x -> m { rem_counters = x })

rem_flagsL :: Lens' (Op Map) (Set.Set ByteString)
rem_flagsL = lens rem_flags (\m x -> m { rem_flags = x })

rem_mapsL :: Lens' (Op Map) (Set.Set ByteString)
rem_mapsL = lens rem_maps (\m x -> m { rem_maps = x })

rem_registersL :: Lens' (Op Map) (Set.Set ByteString)
rem_registersL = lens rem_registers (\m x -> m { rem_registers = x })

rem_setsL :: Lens' (Op Map) (Set.Set ByteString)
rem_setsL = lens rem_sets (\m x -> m { rem_sets = x })


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
updateCounter = updateMapWith upd_countersL

-- | Enable 'Flag' operation.
enableFlag :: NonEmpty ByteString -> Op Map
enableFlag names = updateMapWith upd_flagsL names (Flag True)

-- | Disable 'Flag' operation.
disableFlag :: NonEmpty ByteString -> Op Map
disableFlag names = updateMapWith upd_flagsL names (Flag False)

-- | Set 'Register' operation.
setRegister :: NonEmpty ByteString -> Register -> Op Map
setRegister = updateMapWith upd_registersL

-- | Update 'Set' operation.
updateSet :: NonEmpty ByteString -> Op Set -> Op Map
updateSet = updateMapWith upd_setsL

-- | Internal update 'Map' operation. Not exported, because the only way to
-- update a 'Map' is by updating a 'Counter', 'Flag', 'Register', or 'Set'
-- inside of it.
updateMapWith
  :: Lens' (Op Map) (Map.Map ByteString a) -> NonEmpty ByteString -> a -> Op Map
updateMapWith l xs0 y = go (NonEmpty.toList xs0)
  where
    go :: [ByteString] -> Op Map
    go [x]    = mempty & l .~ Map.singleton x y
    go (x:xs) = mempty & upd_mapsL .~ Map.singleton x (go xs)
    go _      = error "Network.Riak.CRDT.Map.updateMapWith: empty list"

-- | Remove 'Counter' operation.
removeCounter :: NonEmpty ByteString -> Op Map
removeCounter = removeFromMap rem_countersL

-- | Remove 'Flag' operation.
removeFlag :: NonEmpty ByteString -> Op Map
removeFlag = removeFromMap rem_flagsL

-- | Remove 'Map' operation.
removeMap :: NonEmpty ByteString -> Op Map
removeMap = removeFromMap rem_mapsL

-- | Remove 'Register' operation.
removeRegister :: NonEmpty ByteString -> Op Map
removeRegister = removeFromMap rem_registersL

-- | Remove 'Set' operation.
removeSet :: NonEmpty ByteString -> Op Map
removeSet = removeFromMap rem_setsL

removeFromMap
  :: Lens' (Op Map) (Set.Set ByteString) -> NonEmpty ByteString -> Op Map
removeFromMap l = go . NonEmpty.toList
  where
    go :: [ByteString] -> Op Map
    go [x]    = mempty & l .~ Set.singleton x
    go (x:xs) = mempty & upd_mapsL .~ Map.singleton x (go xs)
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
    go = over _1 mapEntriesToMap

    mapEntriesToMap :: Seq MapEntry -> Map
    mapEntriesToMap = foldr' step mempty
      where
        step :: MapEntry -> Map -> Map
        step entry =
          case MapEntry.field entry of
            MapField name MapFieldType.COUNTER ->
              case MapEntry.counter_value entry of
                Nothing -> id
                Just x -> over countersL (Map.insert name (Counter x))

            MapField name MapFieldType.FLAG ->
              case MapEntry.flag_value entry of
                Nothing -> id
                Just x -> over flagsL (Map.insert name (Flag x))

            MapField name MapFieldType.MAP ->
              over mapsL (Map.insert name x)
              where
                x :: Map
                x = mapEntriesToMap (MapEntry.map_value entry)

            MapField name MapFieldType.REGISTER ->
              case MapEntry.register_value entry of
                Nothing -> id
                Just x -> over registersL (Map.insert name (Register x))

            MapField name MapFieldType.SET ->
              over setsL (Map.insert name x)
              where
                x :: Set
                x = Set (seqToSet (MapEntry.set_value entry))
