-- |
-- Module:      Network.Riak.CRDT
-- Copyright:   (c) 2016 Sentenai
-- Author:      Antonio Nikishaev <me@lelf.lu>, Mitchell Rosen <mitchellwrosen@gmail.com>
-- Stability:   experimental
--
-- CRDTs

module Network.Riak.CRDT
  ( -- * CRDT types
    CRDT
  , CRDTException(..)
  , Context
  , Op
  , (><)
    -- * Haskell updates
  , modify
    -- * Riak updates
  , sendModify
  , sendModifyCtx
  , updateRequest
    -- * Fetching data
  , fetchRaw
  , fetchRequest
  ) where

import Network.Riak.CRDT.Internal
