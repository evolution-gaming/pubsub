import sbt._

object Dependencies {

  val `safe-actor`         = "com.evolutiongaming" %% "safe-actor"         % "3.0.0"
  val `akka-serialization` = "com.evolutiongaming" %% "akka-serialization" % "1.0.4"
  val nel                  = "com.evolutiongaming" %% "nel"                % "1.3.4"
  val `metric-tools`       = "com.evolutiongaming" %% "metric-tools"       % "2.0.0"
  val `cats-helper`        = "com.evolutiongaming" %% "cats-helper"        % "2.12.0"
  val scache               = "com.evolution"       %% "scache"             % "3.7.2"
  val scalatest            = "org.scalatest"       %% "scalatest"          % "3.2.3"
  val scalax               = "com.github.t3hnar"   %% "scalax"             % "3.8.1"

  object Akka {
    private val version = "2.6.20"
    val Actor        = "com.typesafe.akka" %% "akka-actor"         % version
    val ClusterTools = "com.typesafe.akka" %% "akka-cluster-tools" % version
    val Testkit      = "com.typesafe.akka" %% "akka-testkit"       % version
    val Stream       = "com.typesafe.akka" %% "akka-stream"        % version
  }

  object Scodec {
    val core = "org.scodec" %% "scodec-core" % "1.11.6"
    val bits = "org.scodec" %% "scodec-bits" % "1.1.14"
  }

  object Cats {
    val core   = "org.typelevel" %% "cats-core"   % "2.9.0"
    val effect = "org.typelevel" %% "cats-effect" % "2.5.5"
  }
}
