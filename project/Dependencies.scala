import sbt._

object Dependencies {

  val `safe-actor`         = "com.evolutiongaming" %% "safe-actor" % "2.0.4"
  val `akka-serialization` = "com.evolutiongaming" %% "akka-serialization" % "1.0.1"
  val nel                  = "com.evolutiongaming" %% "nel" % "1.3.3"
  val `metric-tools`       = "com.evolutiongaming" %% "metric-tools" % "1.1"
  val sequentially         = "com.evolutiongaming" %% "sequentially" % "1.0.14"
  val scalatest            = "org.scalatest" %% "scalatest" % "3.0.7"
  val scalax               = "com.github.t3hnar" %% "scalax" % "3.4"

  object Akka {
    private val version = "2.5.21"
    val Actor        = "com.typesafe.akka" %% "akka-actor" % version
    val ClusterTools = "com.typesafe.akka" %% "akka-cluster-tools" % version
    val Testkit      = "com.typesafe.akka" %% "akka-testkit" % version
    val Stream       = "com.typesafe.akka" %% "akka-stream" % version
  }

  object Scodec {
    val core = "org.scodec" %% "scodec-core" % "1.11.3"
    val bits = "org.scodec" %% "scodec-bits" % "1.1.9"
  }
}
