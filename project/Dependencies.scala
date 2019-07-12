import sbt._

object Dependencies {

  val `safe-actor`         = "com.evolutiongaming" %% "safe-actor"         % "2.0.5"
  val `akka-serialization` = "com.evolutiongaming" %% "akka-serialization" % "1.0.1"
  val nel                  = "com.evolutiongaming" %% "nel"                % "1.3.3"
  val `metric-tools`       = "com.evolutiongaming" %% "metric-tools"       % "1.1"
  val `cats-helper`        = "com.evolutiongaming" %% "cats-helper"        % "0.0.18"
  val scache               = "com.evolutiongaming" %% "scache"             % "0.0.6"
  val scalatest            = "org.scalatest"       %% "scalatest"          % "3.0.8"
  val scalax               = "com.github.t3hnar"   %% "scalax"             % "3.5"

  object Akka {
    private val version = "2.5.23"
    val Actor        = "com.typesafe.akka" %% "akka-actor"         % version
    val ClusterTools = "com.typesafe.akka" %% "akka-cluster-tools" % version
    val Testkit      = "com.typesafe.akka" %% "akka-testkit"       % version
    val Stream       = "com.typesafe.akka" %% "akka-stream"        % version
  }

  object Scodec {
    val core = "org.scodec" %% "scodec-core" % "1.11.4"
    val bits = "org.scodec" %% "scodec-bits" % "1.1.12"
  }

  object Cats {
    private val version = "1.6.1"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % "1.3.1"
  }
}
