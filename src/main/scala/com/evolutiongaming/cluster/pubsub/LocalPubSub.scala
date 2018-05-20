package com.evolutiongaming.cluster.pubsub

import akka.actor._
import akka.cluster.pubsub.{DistributedPubSubMediator => Mediator}
import com.github.t3hnar.scalax.RichSetMap

class LocalPubSub extends Actor with ActorLogging {
  import LocalPubSub._

  var map: Map[String, Set[ActorRef]] = Map()

  def receive: Receive = {
    case Mediator.Publish(topic, msg, _) =>
      map.getOrEmpty(topic) foreach { x => x forward msg }

    case Mediator.Subscribe(topic, group, ref) =>
      map = map.updatedSet(topic, _ + ref)
      context watch ref

      if (group contains Ack) ref ! Subscribed(topic)

    case Mediator.Unsubscribe(topic, _, ref) =>
      map = map.updatedSet(topic, _ - ref)

    case Terminated(ref) =>
      context unwatch ref
      map = map.keys.foldLeft(map) {
        (map, topic) => map.updatedSet(topic, _ - ref)
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

  case class Subscribed(topic: String)

  object Subscribed {
    def apply(topic: Class[_]): Subscribed = Subscribed(topic.getName)
  }

  private[cluster] case object GetState
  private[cluster] case class State(value: Map[String, Set[ActorRef]])
}