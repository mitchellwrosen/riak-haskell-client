-- |
-- Module:     Network.Riak.CRDT
-- Copyright:  (c) 2016 Sentenai
-- Maintainer: Antonio Nikishaev <me@lelf.lu>, Mitchell Rosen <mitchellwrosen@gmail.com>
-- Stability:  experimental
--
-- CRDT operations

module Network.Riak.CRDT
  ( -- * CRDT types
    CRDT
  , Op
  , (><)
  , Context
    -- * Haskell updates
  , modify
    -- * Riak updates
  -- , sendModify
  -- , sendModifyCtx
  -- , updateRequest
    -- * Fetching data
  , fetchRaw
  , fetchRequest
    -- * CRDT Exceptions
  , CRDTException(..)
  ) where

import Network.Riak.CRDT.Internal
