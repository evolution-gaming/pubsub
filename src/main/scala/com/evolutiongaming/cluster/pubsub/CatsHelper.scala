package com.evolutiongaming.cluster.pubsub

import cats.{Applicative, Foldable, Monoid, Parallel, ~>}

object CatsHelper {

  implicit class ParallelOps(val self: Parallel.type) extends AnyVal {

    def foldMap[T[_] : Foldable, M[_], A, B: Monoid](ta: T[A])(f: A => M[B])(implicit P: Parallel[M]): M[B] = {
      val applicative: Applicative[P.F] = P.applicative
      val monoid: Monoid[P.F[B]] = Applicative.monoid[P.F, B](applicative, Monoid[B])
      val parallel: M ~> P.F = P.parallel
      val fb = Foldable[T].foldMap(ta)(f.andThen(parallel.apply(_)))(monoid)
      P.sequential(fb)
    }
  }


  implicit class TOps[T[_], A](val self: T[A]) extends AnyVal {

    def parFoldMap[M[_], B: Monoid](f: A => M[B])(implicit F: Foldable[T], P: Parallel[M]): M[B] = {
      Parallel.foldMap(self)(f)
    }
  }
}
