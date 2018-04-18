import sbt._

object Dependencies {

  object Akka {
    private val version = "2.5.12"
    val Actor        = "com.typesafe.akka" %% "akka-actor" % version
    val ClusterTools = "com.typesafe.akka" %% "akka-cluster-tools" % version
    val Testkit      = "com.typesafe.akka" %% "akka-testkit" % version
    val Stream       = "com.typesafe.akka" %% "akka-stream" % version
  }

  val safeAkka = "com.evolutiongaming" %% "safe-actor" % "1.6"

  val scalax = "com.github.t3hnar" %% "scalax" % "3.3"

  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"

  val nel = "com.evolutiongaming" %% "nel" % "1.2"

  val metricTools = "com.evolutiongaming" %% "metric-tools" % "1.1"

  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
}
