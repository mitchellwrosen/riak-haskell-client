{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}

-- |
-- Module:      Network.Riak.CRDT.Internal
-- Copyright:   (c) 2016 Sentenai
-- Author:      Antonio Nikishaev <me@lelf.lu>, Mitchell Rosen <mitchellwrosen@gmail.com>
-- Stability:   experimental

module Network.Riak.CRDT.Set
  ( -- * Set type
    Set(..)
    -- * Set operations
  , add
  , remove
    -- * Set fetch
  , fetch
  , fetchWith
  ) where

import           Control.Applicative
import           Control.DeepSeq (NFData)
import           Data.ByteString.Lazy (ByteString)
import           Data.Default.Class
import           Data.Foldable
import           Data.Semigroup
import           Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import qualified Data.Set as Set
import           GHC.Generics (Generic)
import           Network.Riak.CRDT.Internal
import           Network.Riak.Lens
import           Network.Riak.Protocol.DtOp (DtOp)
import qualified Network.Riak.Protocol.DtOp as DtOp
import           Network.Riak.Protocol.SetOp (SetOp(SetOp))
import           Network.Riak.Protocol.DtFetchRequest (DtFetchRequest)
import qualified Network.Riak.Protocol.DtFetchResponse.DataType as DtFetchResponse
import qualified Network.Riak.Protocol.DtValue as DtValue
import           Network.Riak.Types hiding (bucket, key)
import qualified Text.ProtocolBuffers as Proto


newtype Set = Set (Set.Set ByteString)
  deriving (Eq, Ord, Show, Generic, Monoid, Semigroup)

instance NFData Set

instance Default Set where
  def = mempty

instance CRDT Set where
  data UOp Set
    = SetMod (Set.Set ByteString) (Set.Set ByteString)
    deriving (Eq, Show)

  type UpdateOp Set = SetOp

  modifyU :: UOp Set -> Set -> Set
  modifyU (SetMod xs ys) (Set zs) = Set ((zs <> xs) Set.\\ ys)

  updateOp :: UOp Set -> UpdateOp Set
  updateOp (SetMod xs ys) = SetOp (toSeq xs) (toSeq ys)
    where
      toSeq :: Set.Set a -> Seq a
      toSeq = Seq.fromList . Set.toList

  unionOp :: UOp Set -> DtOp
  unionOp op = Proto.defaultValue { DtOp.set_op = Just (updateOp op) }

instance Semigroup (UOp Set) where
  SetMod as bs <> SetMod cs ds = SetMod (as <> cs) (bs <> ds)


-- | Add operation.
add :: ByteString -> Op Set 'False
add x = Op (SetMod (Set.singleton x) mempty)

-- | Remove operation.
remove :: ByteString -> Op Set 'True
remove x = Op (SetMod mempty (Set.singleton x))


-- | Fetch a 'Set'. This uses the default 'DtFetchRequest.DtFetchRequest' as
-- returned by 'fetchRequest'.
--
-- Throws 'CRDTTypeMismatch' if the given bucket type, bucket, and key does not
-- contain a 'Set'.
fetch :: Connection -> BucketType -> Bucket -> Key -> IO (Maybe (Set, Context))
fetch conn typ bucket key =
  fmap go <$> fetchWith conn (fetchRequest typ bucket key)
  where
    go :: (Set, Maybe Context) -> (Set, Context)
    go (s, Just c) = (s, c)
    -- @include_context@ was not included in request, so it should default to
    -- true. Therefore, we should always get a context back.
    go _ = error "impossible"

-- | Fetch a 'Set' with the given 'DtFetchRequest.DtFetchRequest'.
--
-- Throws 'CRDTTypeMismatch' if the given bucket type, bucket, and key does
-- not contain a 'Set'.
fetchWith :: Connection -> DtFetchRequest -> IO (Maybe (Set, Maybe Context))
fetchWith conn req =
  fmap go <$> fetchInternal DtFetchResponse.SET DtValue.set_value conn req
  where
    go :: (Seq ByteString, Maybe Context) -> (Set, Maybe Context)
    go = over _1 (Set . seqToSet)
