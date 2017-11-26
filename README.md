# PubSub [![Build Status](https://travis-ci.org/evolution-gaming/pubsub.svg)](https://travis-ci.org/evolution-gaming/pubsub) [![Coverage Status](https://coveralls.io/repos/evolution-gaming/pubsub/badge.svg)](https://coveralls.io/r/evolution-gaming/pubsub) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/5c1e3dc82255463f82583a3fa69fd56f)](https://www.codacy.com/app/evolution-gaming/pubsub?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/pubsub&amp;utm_campaign=Badge_Grade) [![version](https://api.bintray.com/packages/evolutiongaming/maven/pubsub/images/download.svg)](https://bintray.com/evolutiongaming/maven/pubsub/_latestVersion)


### Typesafe layer for DistributedPubSubMediator

```scala
trait PubSub {

  def publish[T](msg: WithSender[T], sendToEachGroup: Boolean = false)(implicit topic: Topic[T]): Unit

  def subscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit

  def subscribe[T](factory: ActorRefFactory)(f: (T, ActorRef) => Unit)(implicit topic: Topic[T]): Unit

  def unsubscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit

  def topics(sender: ActorRef): Unit
}
```

### Ability to serialize/deserialize messages to offload akka remoting and improve throughput

Check `DistributedPubSubMediatorSerializing.scala`


## Setup

```scala
resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies += "com.evolutiongaming" %% "pubsub" % "1.0"
```
