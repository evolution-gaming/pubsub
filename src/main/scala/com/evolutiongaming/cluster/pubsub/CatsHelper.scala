package com.evolutiongaming.cluster.pubsub

import cats.{Applicative, Foldable, Monoid, Parallel}

object CatsHelper {

  implicit class ParallelOps(val self: Parallel.type) extends AnyVal {

    def foldMap[T[_] : Foldable, M[_], F[_], A, B: Monoid](ta: T[A])(f: A => M[B])(implicit P: Parallel[M, F]): M[B] = {
      implicit val applicative = P.applicative
      implicit val monoid = Applicative.monoid[F, B]
      val fb = Foldable[T].foldMap(ta)(f.andThen(P.parallel.apply))
      P.sequential(fb)
    }
  }


  implicit class TOps[T[_], A](val self: T[A]) extends AnyVal {

    def parFoldMap[M[_], F[_], B: Monoid](f: A => M[B])(implicit F: Foldable[T], P: Parallel[M, F]): M[B] = {
      Parallel.foldMap(self)(f)
    }
  }
}
