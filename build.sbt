import Dependencies._

name := "pubsub"

organization := "com.evolutiongaming"

homepage := Some(new URL("http://github.com/evolution-gaming/pubsub"))

startYear := Some(2017)

organizationName := "Evolution"

organizationHomepage := Some(url("http://evolution.com"))

scalaVersion := crossScalaVersions.value.head

crossScalaVersions := Seq("2.13.5", "2.12.19")

publishTo := Some(Resolver.evolutionReleases)

libraryDependencies ++= Seq(
  Akka.Actor,
  Akka.Stream,
  Akka.ClusterTools,
  Akka.Testkit % Test,
  Scodec.core,
  Scodec.bits,
  Cats.core,
  Cats.effect,
  `safe-actor`,
  scalax,
  `metric-tools`,
  `akka-serialization`,
  `cats-helper`,
  scache,
  scalatest % Test)

licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT")))

releaseCrossBuild := true

Compile / doc / scalacOptions ++= Seq("-groups", "-implicits", "-no-link-warnings")