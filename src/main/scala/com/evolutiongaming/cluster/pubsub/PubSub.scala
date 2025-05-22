package com.evolutiongaming.cluster.pubsub

import akka.actor.{ActorPath, ActorRef, ActorRefFactory, ActorSystem}
import akka.cluster.Cluster
import akka.cluster.pubsub.{DistributedPubSubMediatorSerializing, DistributedPubSubMediator => Mediator}
import akka.pattern._
import akka.util.Timeout
import cats.effect.{Resource, Sync}
import cats.syntax.all._
import cats.{Applicative, Id, Monad, ~>}
import com.codahale.metrics.MetricRegistry
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{FromFuture, ToFuture, ToTry}
import com.evolutiongaming.metrics.MetricName
import com.evolutiongaming.safeakka.actor.{ActorCtx, ActorLog, Behavior, SafeActorRef, SetupActor, Signal, Unapply}
import com.evolutiongaming.serialization.ToBytesAble
import scodec.bits.ByteVector

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag

trait PubSub[F[_]] {
  import PubSub._

  def publish[A: Topic : ToBytes](
    msg: A,
    sender: Option[ActorRef] = None,
    sendToEachGroup: Boolean = false
  ): F[Unit]

  def subscribe[A: Topic : FromBytes : ClassTag](
    group: Option[String] = None)(
    onMsg: OnMsg[F, A]
  ): Resource[F, Unit]

  def topics(timeout: FiniteDuration = 3.seconds): F[Set[String]]
}

object PubSub {

  type OnMsg[F[_], -A] = (A, ActorPath) => F[Unit]


  def empty[F[_] : Applicative]: PubSub[F] = const(Set.empty[String].pure[F], ().pure[F])


  def const[F[_]](topics: F[Set[String]], unit: F[Unit]): PubSub[F] = {
    val topics1 = topics
    new PubSub[F] {

      def publish[A: Topic : ToBytes](msg: A, sender: Option[ActorRef], sendToEachGroup: Boolean) = unit

      def subscribe[A: Topic : FromBytes : ClassTag](group: Option[String])(onMsg: OnMsg[F, A]) = {
        Resource.eval(unit)
      }

      def topics(timeout: FiniteDuration) = topics1
    }
  }


  /**
   * Initializes a cluster-local pubsub. If cluster is not initialized, starts a node-local pubsub.
   */
  def of[F[_] : Sync : ToTry : ToFuture : FromFuture](
    system: ActorSystem,
    metrics: Metrics[F],
    serialize: String => Boolean = _ => false
  ): Resource[F, PubSub[F]] = {
    val toTry: F ~> Id = new (F ~> Id) {
      def apply[A](fa: F[A]): A = fa.toTry.get
    }
    for {
      hasCluster <- Resource.eval(Sync[F].delay(system.hasExtension(Cluster)))
      actorRef <- {
        val metrics1 = metrics.mapK(toTry)
        val actorRef = Sync[F].delay {
          if (hasCluster) DistributedPubSubMediatorSerializing(system, serialize, metrics1)
          else            system.actorOf(LocalPubSub.props)
        }
        Resource.make(actorRef) { ref => Sync[F].delay { system.stop(ref) } }
      }
      log <- Resource.eval(Sync[F].delay(ActorLog(system, PubSub.getClass)))
    } yield {
      apply(actorRef, log, system)
    }
  }

  private class PubSubCluster[F[_]: Sync: ToFuture: FromFuture](pubSub: ActorRef, log: ActorLog, factory: ActorRefFactory) extends PubSub[F] {
    override def publish[A](msg: A, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false)
                  (implicit topic: Topic[A], toBytes: ToBytes[A]): F[Unit] = {

      val toBytesAble = ToBytesAble(msg)(toBytes.apply)
      val publish = Mediator.Publish(topic.name, toBytesAble, sendToEachGroup)
      Sync[F].delay {
        log.debug(s"publish $publish")
        pubSub.tell(publish, sender getOrElse ActorRef.noSender)
      }
    }

    override def subscribe[A](group: Option[String] = None)(onMsg: OnMsg[F, A])
                             (implicit topic: Topic[A], fromBytes: FromBytes[A], tag: ClassTag[A]): Resource[F, Unit] = {
      implicit val topicFinal = Topic[ToBytesAble](topic.name)

      def onBytes(bytes: ByteVector, sender: ActorPath) = {
        for {
          a <- Sync[F].delay { fromBytes(bytes) }
          a <- onMsg(a, sender)
        } yield a
      }

      val onToBytesAble: OnMsg[F, ToBytesAble] = (msg: ToBytesAble, sender: ActorPath) => {
        msg match {
          case ToBytesAble.Bytes(bytes)  => onBytes(bytes, sender)
          case ToBytesAble.Raw(tag(msg)) => onMsg(msg, sender)
          case ToBytesAble.Raw(msg)      => Sync[F].delay { log.warn(s"$topic: receive unexpected $msg") }
        }
      }

      subscribeRaw[ToBytesAble](group)(onToBytesAble)
    }

    override def topics(timeout: FiniteDuration): F[Set[String]] = {
      implicit val timeout1 = Timeout(timeout)
      for {
        a <- FromFuture[F].apply { pubSub.ask(Mediator.GetTopics).mapTo[Mediator.CurrentTopics] }
      } yield a.topics
    }

    def subscribeRaw[A](
                         group: Option[String])(
                         onMsg: OnMsg[F, A],
                       )(implicit
                         topic: Topic[A],
                         tag: ClassTag[A]
                       ) = {

      import Subscription.In

      def subscribe(log: ActorLog) = {

        val setup: SetupActor[In[A]] = (ctx: ActorCtx) => {

          implicit val executor = ctx.dispatcher

          def behavior(state: Future[Unit]): Behavior[In[A]] = {
            Behavior[In[A]] {
              case Signal.Msg(msg, sender) => msg match {
                case In.Subscribed   =>
                  log.debug(s"subscribed ${ ctx.self }")
                  behavior(state)

                case In.Unsubscribed =>
                  log.debug(s"unsubscribed ${ ctx.self }")
                  behavior(state)

                case In.Msg(msg)     =>
                  log.debug(s"receive $msg")
                  val fa = onMsg(msg, sender.path).handleErrorWith { error =>
                    Sync[F].delay { log.error(s"failed to receive $msg: $error", error) }
                  }

                  val state1 = for {
                    _ <- state
                    _ <- fa.toFuture
                  } yield {}
                  behavior(state1)
              }
              case Signal.PostStop         =>
                val unsubscribe = Mediator.Unsubscribe(topic.name, group, ctx.self)
                log.debug(s"unsubscribe $unsubscribe")
                pubSub.tell(unsubscribe, ctx.self)
                behavior(state)

              case _                       =>
                behavior(state)
            }
          }

          val logListener = ActorLog(ctx.system, PubSub.getClass) prefixed topic.name
          (behavior(Future.unit), logListener)
        }

        val ref: SafeActorRef[In[A]] = SafeActorRef(setup)(factory, Subscription.In.unapplyOf[A])
        val subscribe = Mediator.Subscribe(topic.name, group, ref.unsafe)
        log.debug(s"subscribe $subscribe")
        pubSub.tell(subscribe, ref.unsafe)
        () => factory.stop(ref.unsafe)
      }

      val result = for {
        unsubscribe <- Sync[F].delay { subscribe(log.prefixed(topic.name)) }
      } yield {
        val release = Sync[F].delay { unsubscribe() }
        ((), release)
      }
      Resource(result)
    }
  }

  def apply[F[_] : Sync : ToFuture : FromFuture](pubSub: ActorRef, log: ActorLog, factory: ActorRefFactory): PubSub[F] = {
    new PubSubCluster[F](pubSub, log, factory)
  }

  def proxy[F[_] : Sync : FromFuture](actorRef: ActorRef): PubSub[F] = new PubSub[F] {

    def publish[A: Topic : ToBytes](msg: A, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false) = {
      val sender1 = sender getOrElse ActorRef.noSender
      Sync[F].delay { actorRef.tell(msg, sender1) }
    }

    def subscribe[A: Topic : FromBytes : ClassTag](group: Option[String] = None)(onMsg: OnMsg[F, A]) = {
      Resource.pure[F, Unit](())
    }

    def topics(timeout: FiniteDuration) = {
      implicit val timeout1 = Timeout(timeout)
      for {
        a <- FromFuture[F].apply { actorRef.ask(Mediator.GetTopics).mapTo[Mediator.CurrentTopics] }
      } yield {
        a.topics
      }
    }
  }


  object Subscription {

    sealed trait In[+A]

    object In {

      def unapplyOf[A](implicit tag: ClassTag[A]): Unapply[In[A]] = Unapply.pf[In[A]] {
        case In.Msg(tag(x))             => In.Msg(x)
        case _: Mediator.SubscribeAck   => In.Subscribed
        case _: Mediator.UnsubscribeAck => In.Unsubscribed
        case In.Subscribed              => In.Subscribed
        case In.Unsubscribed            => In.Unsubscribed
        case tag(x)                     => In.Msg(x)
      }

      final case class Msg[+A](msg: A) extends In[A]
      case object Subscribed extends In[Nothing]
      case object Unsubscribed extends In[Nothing]
    }
  }


  trait Metrics[F[_]] {

    def subscribe(topic: String): F[Unit]

    def unsubscribe(topic: String): F[Unit]

    def publish(topic: String): F[Unit]

    def toBytes(topic: String, size: Long): F[Unit]

    def fromBytes(topic: String, size: Long): F[Unit]

    def latency(topic: String, latencyMs: Long): F[Unit]
  }

  object Metrics {

    def empty[F[_] : Applicative]: Metrics[F] = const(().pure[F])


    def const[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def subscribe(topic: String) = unit

      def unsubscribe(topic: String) = unit

      def publish(topic: String) = unit

      def toBytes(topic: String, size: Long) = unit

      def fromBytes(topic: String, size: Long) = unit

      def latency(topic: String, latencyMs: Long) = unit
    }


    def codahale[F[_] : Sync](registry: MetricRegistry): F[Metrics[F]] = {

      def nameOf(topic: String) = Sync[F].delay { MetricName(topic) }

      val toBytesMeter = Sync[F].delay { registry.meter("toBytes") }

      val fromBytesMeter = Sync[F].delay { registry.meter("fromBytes") }

      val latencyHistogram = Sync[F].delay { registry.histogram("latency") }

      for {
        toBytesMeter     <- toBytesMeter
        fromBytesMeter   <- fromBytesMeter
        latencyHistogram <- latencyHistogram
      } yield {
        new Metrics[F] {

          def subscribe(topic: String) = {
            for {
              name <- nameOf(topic)
              _    <- Sync[F].delay { registry.counter(s"$name.subscriptions").inc() }
            } yield {}
          }

          def unsubscribe(topic: String) = {
            for {
              name <- nameOf(topic)
              _    <- Sync[F].delay { registry.counter(s"$name.subscriptions").dec() }
            } yield {}
          }

          def publish(topic: String) = {
            for {
              name <- nameOf(topic)
              _    <- Sync[F].delay { registry.meter(s"$name.publish").mark() }
            } yield {}
          }

          def toBytes(topic: String, size: Long) = {
            for {
              name <- nameOf(topic)
              _    <- Sync[F].delay { toBytesMeter.mark(size) }
              _    <- Sync[F].delay { registry.meter(s"$name.toBytes").mark(size) }
            } yield {}
          }

          def fromBytes(topic: String, size: Long) = {
            for {
              name <- nameOf(topic)
              _    <- Sync[F].delay { fromBytesMeter.mark(size) }
              _    <- Sync[F].delay { registry.meter(s"$name.fromBytes").mark(size) }
            } yield {}
          }

          def latency(topic: String, latencyMs: Long) = {
            for {
              name <- nameOf(topic)
              _    <- Sync[F].delay { latencyHistogram.update(latencyMs) }
              _    <- Sync[F].delay { registry.histogram(s"$name.latency").update(latencyMs) }
            } yield {}
          }
        }
      }
    }


    implicit class MetricsOps[F[_]](val self: Metrics[F]) extends AnyVal {

      def mapK[G[_]](f: F ~> G): Metrics[G] = new Metrics[G] {

        def subscribe(topic: String) = f(self.subscribe(topic))

        def unsubscribe(topic: String) = f(self.unsubscribe(topic))

        def publish(topic: String) = f(self.publish(topic))

        def toBytes(topic: String, size: Long) = f(self.toBytes(topic, size))

        def fromBytes(topic: String, size: Long) = f(self.fromBytes(topic, size))

        def latency(topic: String, latencyMs: Long) = f(self.latency(topic, latencyMs))
      }
    }
  }


  implicit class PubSubOps[F[_]](val self: PubSub[F]) extends AnyVal {

    def withOptimiseSubscribe(optimiseSubscribe: OptimiseSubscribe[F]): PubSub[F] = {
      new PubSub[F] {

        def publish[A: Topic : ToBytes](msg: A, sender: Option[ActorRef], sendToEachGroup: Boolean) = {
          self.publish(msg, sender, sendToEachGroup)
        }

        def subscribe[A: Topic : FromBytes : ClassTag](group: Option[String])(onMsg: OnMsg[F, A]) = {
          optimiseSubscribe[A](onMsg) { onMsg =>
            self.subscribe[A](group)(onMsg)
          }
        }

        def topics(timeout: FiniteDuration) = self.topics(timeout)
      }
    }


    def withMetrics(metrics: Metrics[F])(implicit F: Monad[F]): PubSub[F] = {

      new PubSub[F] {

        def publish[A](
          msg: A,
          sender: Option[ActorRef] = None,
          sendToEachGroup: Boolean = false)(implicit
          topic: Topic[A],
          toBytes: ToBytes[A]
        ) = {
          for {
            a <- self.publish(msg, sender, sendToEachGroup)
            _ <- metrics.publish(topic.name)
          } yield a
        }

        def subscribe[A](
          group: Option[String] = None)(
          onMsg: OnMsg[F, A])(implicit
          topic: Topic[A],
          fromBytes: FromBytes[A],
          tag: ClassTag[A]
        ) = {

          val name = topic.name
          for {
            _ <- Resource.make { metrics.subscribe(name) } { _ => metrics.unsubscribe(name) }
            a <- self.subscribe(group)(onMsg)
          } yield a
        }

        def topics(timeout: FiniteDuration) = self.topics(timeout)
      }
    }
  }
}
