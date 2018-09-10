# PubSub [![Build Status](https://travis-ci.org/evolution-gaming/pubsub.svg)](https://travis-ci.org/evolution-gaming/pubsub) [![Coverage Status](https://coveralls.io/repos/evolution-gaming/pubsub/badge.svg)](https://coveralls.io/r/evolution-gaming/pubsub) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/5c1e3dc82255463f82583a3fa69fd56f)](https://www.codacy.com/app/evolution-gaming/pubsub?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/pubsub&amp;utm_campaign=Badge_Grade) [![version](https://api.bintray.com/packages/evolutiongaming/maven/pubsub/images/download.svg)](https://bintray.com/evolutiongaming/maven/pubsub/_latestVersion) [![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)


### Typesafe layer for DistributedPubSubMediator

```scala
trait PubSub {

  def publishAny[T: Topic](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false): Unit

  def subscribeAny[T: Topic: ClassTag](ref: ActorRef, group: Option[String] = None): Unsubscribe

  def subscribeAny[T: Topic: ClassTag](factory: ActorRefFactory)(onMsg: OnMsg[T]): Unsubscribe

  def subscribeAny[T: Topic: ClassTag](factory: ActorRefFactory, group: Option[String])(onMsg: OnMsg[T]): Unsubscribe

  def publish[T: Topic: ToBytes](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false): Unit

  def subscribe[T: Topic: FromBytes: ClassTag](factory: ActorRefFactory, group: Option[String] = None)(onMsg: OnMsg[T]): Unsubscribe

  def unsubscribe[T: Topic](ref: ActorRef, group: Option[String] = None): Unit

  def topics(timeout: FiniteDuration = 3.seconds): Future[Set[String]]
}
```

### Ability to serialize/deserialize messages to offload akka remoting and improve throughput

Check `DistributedPubSubMediatorSerializing.scala`


## Setup

```scala
resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies += "com.evolutiongaming" %% "pubsub" % "2.1.0"
```
