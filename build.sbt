import Dependencies._

name := "pubsub"

organization := "com.evolutiongaming"

homepage := Some(url("https://github.com/evolution-gaming/pubsub"))

startYear := Some(2017)

organizationName := "Evolution"

organizationHomepage := Some(url("https://evolution.com"))

scalaVersion := crossScalaVersions.value.head

crossScalaVersions := Seq("2.13.15", "2.12.13")

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

//addCommandAlias("check", "all versionPolicyCheck Compile/doc")
addCommandAlias("check", "show version")
addCommandAlias("build", "+all compile test")
