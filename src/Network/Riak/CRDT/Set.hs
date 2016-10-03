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
  data Op Set
    = SetMod (Set.Set ByteString) (Set.Set ByteString)
    deriving (Eq, Show)

  modify :: Op Set -> Set -> Set
  modify (SetMod xs ys) (Set zs) = Set ((zs <> xs) Set.\\ ys)

instance Semigroup (Op Set) where
  SetMod as bs <> SetMod cs ds = SetMod (as <> cs) (bs <> ds)

instance CRDTOp (Op Set) where
  type UpdateOp (Op Set) = SetOp

  updateOp :: Op Set -> UpdateOp (Op Set)
  updateOp (SetMod xs ys) = SetOp (toSeq xs) (toSeq ys)
    where
      toSeq :: Set.Set a -> Seq a
      toSeq = Seq.fromList . Set.toList

  unionOp :: Op Set -> DtOp
  unionOp op = Proto.defaultValue { DtOp.set_op = Just (updateOp op) }


-- | Add operation.
add :: ByteString -> Op Set
add x = SetMod (Set.singleton x) mempty

-- | Remove operation. A request to Riak containing any 'Set' removals
-- (including removing from 'Set's stored inside of 'Network.Riak.CRDT.Map's)
-- must include a 'Context' or it will fail.
--
-- @
-- -- BAD! Does not set context!
-- 'sendModify' conn typ bucket key ('remove' val)
--
-- -- GOOD! Sets context!
-- 'sendModifyCtx' conn typ bucket key ctx ('remove' val)
-- @
remove :: ByteString -> Op Set
remove x = SetMod mempty (Set.singleton x)


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
    go (xs, ctx) = (xs', ctx)
      where
        xs' :: Set
        xs' = Set (foldr' Set.insert mempty xs)
