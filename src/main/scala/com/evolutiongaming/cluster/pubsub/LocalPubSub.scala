package com.evolutiongaming.cluster.pubsub

import akka.actor._
import akka.cluster.pubsub.{DistributedPubSubMediator => Mediator}

class LocalPubSub extends Actor with ActorLogging {
  import LocalPubSub._

  var map: Map[String, Set[ActorRef]] = Map()

  private def getSet(topic: String): Set[ActorRef] =
    map.getOrElse(topic, Set.empty)

  private def update(
    map: Map[String, Set[ActorRef]],
    topic: String,
    withSet: Set[ActorRef],
  ): Map[String, Set[ActorRef]] =
    if (withSet.isEmpty) map.removed(topic)
    else map.updated(topic, withSet)

  def receive: Receive = {
    case Mediator.Publish(topic, msg, _) =>
      getSet(topic) foreach { x => x forward msg }

    case Mediator.Subscribe(topic, group, ref) =>
      val set = getSet(topic) + ref
      map = update(map, topic, set)
      context watch ref

      if (group contains Ack) ref ! Subscribed(topic)

    case Mediator.Unsubscribe(topic, _, ref) =>
      val set = getSet(topic) - ref
      map = update(map, topic, set)

    case Terminated(ref) =>
      context unwatch ref
      map = map.keys.foldLeft(map) { (map, topic) =>
        val set = getSet(topic) - ref
        update(map, topic, set)
      }

    case GetState => sender() ! State(map)

    case Mediator.Count => sender() ! map.values.foldLeft(0) { _ + _.size }

    case Mediator.GetTopics => sender() ! Mediator.CurrentTopics(map.keySet)
  }
}

object LocalPubSub {
  def props: Props = Props(new LocalPubSub)

  // to be used in tests only, will not work in cluster
  private[cluster] val Ack: String = "ack"

  final case class Subscribed(topic: String)

  object Subscribed {
    def apply(topic: Class[?]): Subscribed = Subscribed(topic.getName)
  }

  private[cluster] case object GetState
  private[cluster] final case class State(value: Map[String, Set[ActorRef]])
}