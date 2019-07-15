# PubSub [![Build Status](https://travis-ci.org/evolution-gaming/pubsub.svg)](https://travis-ci.org/evolution-gaming/pubsub) [![Coverage Status](https://coveralls.io/repos/evolution-gaming/pubsub/badge.svg)](https://coveralls.io/r/evolution-gaming/pubsub) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/5c1e3dc82255463f82583a3fa69fd56f)](https://www.codacy.com/app/evolution-gaming/pubsub?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/pubsub&amp;utm_campaign=Badge_Grade) [![version](https://api.bintray.com/packages/evolutiongaming/maven/pubsub/images/download.svg)](https://bintray.com/evolutiongaming/maven/pubsub/_latestVersion) [![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)


### Typesafe layer for DistributedPubSubMediator

```scala
trait PubSub[F[_]] {

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
```

### Ability to serialize/deserialize messages to offload akka remoting and improve throughput

Check `DistributedPubSubMediatorSerializing.scala`


## Setup

```scala
resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies += "com.evolutiongaming" %% "pubsub" % "6.0.0"
```
