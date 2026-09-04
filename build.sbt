import Dependencies._

name := "pubsub"

organization := "com.evolutiongaming"

homepage := Some(uri("https://github.com/evolution-gaming/pubsub"))

startYear := Some(2017)

organizationName := "Evolution"

organizationHomepage := Some(uri("https://evolution.com"))

crossScalaVersions := Seq("3.9.0", "3.3.8")

scalaVersion := crossScalaVersions.value.head

versionPolicyIntention := Compatibility.BinaryCompatible

publishTo := Some(Resolver.evolutionReleases)

libraryDependencies ++= Seq(
  Akka.Actor,
  Akka.Stream,
  Akka.ClusterTools,
  Akka.Testkit % Test,
  Scodec.Bits,
  Cats.core,
  Cats.effect,
  `metric-tools`,
  `akka-serialization`,
  `cats-helper`,
  scache,
  scalatest % Test,
)
libraryDependencies ++= crossSettings(
  scalaVersion = scalaVersion.value,
  if2 = Seq(Scodec.Scala2.Core),
  if3 = Seq(Scodec.Scala3.Core),
)

licenses := Seq(("MIT", uri("https://opensource.org/licenses/MIT")))

scalacOptions ++= Seq(
  "-release:17",
  "-deprecation",
)
scalacOptions ++= crossSettings(
  scalaVersion = scalaVersion.value,
  // Good compiler options for Scala 2.13 are coming from com.evolution:sbt-scalac-opts-plugin:0.0.9,
  // but its support for Scala 3 is limited, especially what concerns linting options.
  if2 = Seq(
    "-Xsource:3",
  ),
  // If Scala 3 is made the primary target, good linting scalac options for it should be added first.
  if3 = Seq(
    "-Ykind-projector:underscores",

    // disable new brace-less syntax:
    // https://alexn.org/blog/2022/10/24/scala-3-optional-braces/
    "-no-indent",

    // improve error messages:
    "-explain",
    "-explain-types",
  ),
)

Compile / doc / scalacOptions ++= Seq("-groups", "-implicits", "-no-link-warnings")

versionPolicyIntention := Compatibility.BinaryCompatible

versionPolicyIgnored ++= Seq(
  // add libraries here that are known to be binary compatible, like:
  // TODO remove after next release, this project doesn't use doobie module
  "com.evolutiongaming" %% "smetrics",
)

addCommandAlias("check", "all scalafmtCheckRepo versionPolicyCheck Compile/doc")
addCommandAlias("fmt", "scalafmtRepo")
addCommandAlias("build", "+all compile testFull")

def crossSettings[T](scalaVersion: String, if3: T, if2: T): T = {
  scalaVersion match {
    case version if version.startsWith("3") => if3
    case _ => if2
  }
}
