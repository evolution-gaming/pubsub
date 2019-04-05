package com.evolutiongaming.cluster.pubsub

import com.evolutiongaming.serialization.SerializedMsg

final case class PubSubMsg(serializedMsg: SerializedMsg, timestamp: Long)