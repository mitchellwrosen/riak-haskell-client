-- |
-- Module:      Network.Riak.CRDT.Internal
-- Copyright:   (c) 2016 Sentenai
-- Author:      Antonio Nikishaev <me@lelf.lu>, Mitchell Rosen <mitchellwrosen@gmail.com>
-- Stability:   experimental

module Network.Riak.CRDT
  ( -- * CRDT types
    CRDT(..)
  , CRDTException(..)
    -- * Sending updates
  , sendModify
  , sendModifyCtx
  , updateRequest
    -- * Fetching data
  , fetchRaw
  , fetchRequest
  ) where

import Network.Riak.CRDT.Internal
