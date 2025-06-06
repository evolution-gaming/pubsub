import sbt._

object Dependencies {

  val `akka-serialization` = "com.evolutiongaming" %% "akka-serialization" % "1.0.5"
  val nel                  = "com.evolutiongaming" %% "nel"                % "1.3.5"
  val `metric-tools`       = "com.evolutiongaming" %% "metric-tools"       % "3.0.0"
  val `cats-helper`        = "com.evolutiongaming" %% "cats-helper"        % "3.12.0"
  val scache               = "com.evolution"       %% "scache"             % "5.1.2"
  val scalatest            = "org.scalatest"       %% "scalatest"          % "3.2.9"
  val scalax               = "com.github.t3hnar"   %% "scalax"             % "3.8.1"

  object Akka {
    private val version = "2.6.21"
    val Actor        = "com.typesafe.akka" %% "akka-actor"         % version
    val ClusterTools = "com.typesafe.akka" %% "akka-cluster-tools" % version
    val Testkit      = "com.typesafe.akka" %% "akka-testkit"       % version
    val Stream       = "com.typesafe.akka" %% "akka-stream"        % version
  }

  object Scodec {
    val core = "org.scodec" %% "scodec-core" % "1.11.11"
    val bits = "org.scodec" %% "scodec-bits" % "1.2.1"
  }

  object Cats {
    val core   = "org.typelevel" %% "cats-core"   % "2.13.0"
    val effect = "org.typelevel" %% "cats-effect" % "3.5.7"
  }
}
