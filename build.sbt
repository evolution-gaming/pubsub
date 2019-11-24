import Dependencies._

name := "pubsub"

organization := "com.evolutiongaming"

homepage := Some(new URL("http://github.com/evolution-gaming/pubsub"))

startYear := Some(2017)

organizationName := "Evolution Gaming"

organizationHomepage := Some(url("http://evolutiongaming.com"))

bintrayOrganization := Some("evolutiongaming")

scalaVersion := crossScalaVersions.value.head

crossScalaVersions := Seq("2.13.1", "2.12.10")

resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

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

scalacOptions in(Compile, doc) ++= Seq("-groups", "-implicits", "-no-link-warnings")