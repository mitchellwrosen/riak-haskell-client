{-# LANGUAGE CPP        #-}
{-# LANGUAGE RankNTypes #-}

-- | Just enough lens functionality to be useful without depending on an
-- external library.

module Network.Riak.Lens
  ( Lens
  , Lens'
  , lens
  , (&)
  , (.~)
  , over
  , _1
  ) where

import Control.Applicative
import Data.Functor.Identity

#if MIN_VERSION_base(4,8,0)
import Data.Function ((&))
#else
infixl 1 &
(&) :: a -> (a -> b) -> b
x & f = f x
#endif

type Lens s t a b = forall f. Functor f => (a -> f b) -> s -> f t

type Lens' s a = Lens s s a a

lens :: (s -> a) -> (s -> b -> t) -> Lens s t a b
lens get set f s = set s <$> f (get s)

infixr 4 .~
(.~) :: Lens s t a b -> b -> s -> t
(.~) l a = over l (const a)

over :: Lens s t a b -> (a -> b) -> s -> t
over l f = runIdentity . l (Identity . f)

_1 :: Lens (a, c) (b, c) a b
_1 = lens fst (\(_, c) b -> (b, c))
