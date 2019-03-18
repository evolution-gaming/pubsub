import sbt._

object Dependencies {

  object Akka {
    private val version = "2.5.21"
    val Actor        = "com.typesafe.akka" %% "akka-actor" % version
    val ClusterTools = "com.typesafe.akka" %% "akka-cluster-tools" % version
    val Testkit      = "com.typesafe.akka" %% "akka-testkit" % version
    val Stream       = "com.typesafe.akka" %% "akka-stream" % version
  }

  val `safe-actor` = "com.evolutiongaming" %% "safe-actor" % "2.0.4"

  val `akka-serialization` = "com.evolutiongaming" %% "akka-serialization" % "0.0.4"

  val scalax = "com.github.t3hnar" %% "scalax" % "3.4"

  val nel = "com.evolutiongaming" %% "nel" % "1.3.3"

  val `metric-tools` = "com.evolutiongaming" %% "metric-tools" % "1.1"

  val scalatest = "org.scalatest" %% "scalatest" % "3.0.6"

  val sequentially = "com.evolutiongaming" %% "sequentially" % "1.0.14"
}
