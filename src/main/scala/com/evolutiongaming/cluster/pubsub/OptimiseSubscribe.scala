package com.evolutiongaming.cluster.pubsub

import cats.Parallel
import cats.data.{NonEmptyList => Nel}
import cats.effect.{Concurrent, MonadCancelThrow, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper.Runtime
import com.evolutiongaming.cluster.pubsub.PubSub.OnMsg
import com.evolution.scache.SerialMap

trait OptimiseSubscribe[F[_]] {

  def apply[A: Topic](
    onMsg: OnMsg[F, A])(
    subscribe: OnMsg[F, A] => Resource[F, Unit]
  ): Resource[F, Unit]
}

object OptimiseSubscribe {

  def empty[F[_]]: OptimiseSubscribe[F] = new OptimiseSubscribe[F] {

    def apply[A: Topic](
      onMsg: OnMsg[F, A])(
      subscribe: OnMsg[F, A] => Resource[F, Unit]
    ) = {
      subscribe(onMsg)
    }
  }


  type Listener[F[_]] = OnMsg[F, Any]


  def of[F[_] : Concurrent: Runtime: Parallel]: F[OptimiseSubscribe[F]] = {
    for {
      serialMap <- SerialMap.of[F, String, Subscription[F]]
    } yield {
      apply(serialMap)
    }
  }

  def apply[F[_] : MonadCancelThrow: Parallel](serialMap: SerialMap[F, String, Subscription[F]]): OptimiseSubscribe[F] = {

    new OptimiseSubscribe[F] {

      def apply[A](
        onMsg: OnMsg[F, A])(
        subscribe: OnMsg[F, A] => Resource[F, Unit])(implicit
        topic: Topic[A]
      ) = {

        val listener = onMsg.asInstanceOf[Listener[F]]

        def update(f: Option[Subscription[F]] => F[Option[Subscription[F]]]) = {
          serialMap.update(topic.name)(f)
        }

        def create = {
          val onMsg: OnMsg[F, A] = (a: A, sender) => {
            for {
              subscription <- serialMap.get(topic.name)
              _            <- subscription.foldMapM { _.listeners.parFoldMap1 { listener => listener(a, sender) } }
            } yield {}
          }

          for {
            a <- subscribe(onMsg).allocated
          } yield {
            val (_, unsubscribe) = a
            Subscription[F](unsubscribe, Nel.of(listener))
          }
        }

        val result = for {
          _ <- update { subscription =>
            for {
              subscription <- subscription.fold {
                create
              } { subscription =>
                (subscription + listener).pure[F]
              }
            } yield {
              subscription.some
            }
          }
        } yield {
          val unsubscribe = for {
            _ <- update {
              case None               => none[Subscription[F]].pure[F]
              case Some(subscription) => subscription - listener match {
                case Some(subscription) => subscription.some.pure[F]
                case None               => subscription.unsubscribe as none[Subscription[F]]
              }
            }
          } yield {}
          ((), unsubscribe)
        }

        Resource(result)
      }
    }
  }


  final case class Subscription[F[_]](
    unsubscribe: F[Unit],
    listeners: Nel[Listener[F]]
  ) { self =>

    def +(listener: Listener[F]): Subscription[F] = {
      copy(listeners = listener :: listeners)
    }

    def -(listener: Listener[F]): Option[Subscription[F]] = {
      val listeners = self.listeners.filter(_ != listener)
      for {
        listeners <- Nel.fromList(listeners)
      } yield {
        copy(listeners = listeners)
      }
    }
  }
}
