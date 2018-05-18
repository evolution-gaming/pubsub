import Dependencies._

name := "pubsub"

organization := "com.evolutiongaming"

homepage := Some(new URL("http://github.com/evolution-gaming/pubsub"))

startYear := Some(2017)

organizationName := "Evolution Gaming"

organizationHomepage := Some(url("http://evolutiongaming.com"))

bintrayOrganization := Some("evolutiongaming")

scalaVersion := "2.12.6"

crossScalaVersions := Seq("2.12.6", "2.11.12")

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-deprecation",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture")

scalacOptions in(Compile, doc) ++= Seq("-groups", "-implicits", "-no-link-warnings")

resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies ++= Seq(
  Akka.Actor,
  Akka.Stream,
  Akka.ClusterTools,
  Akka.Testkit % Test,
  safeAkka,
  scalax,
  scalaLogging,
  nel,
  metricTools,
  scalaTest % Test)

licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT")))

releaseCrossBuild := true