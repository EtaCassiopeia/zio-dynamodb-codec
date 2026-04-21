import xerial.sbt.Sonatype.sonatypeCentralHost

val scala3Version    = "3.3.6"
val zioBlocksVersion = "0.0.33"
val awsSdkVersion    = "2.31.61"

ThisBuild / organization := "io.github.etacassiopeia"
ThisBuild / scalaVersion := scala3Version

// Version is derived from git tags by sbt-dynver (via sbt-ci-release)
// Tags: v0.1.0, v1.0.0-RC1, etc.
// Snapshots: 0.1.0+3-abcd1234-SNAPSHOT

ThisBuild / homepage := Some(url("https://github.com/EtaCassiopeia/zio-dynamodb-codec"))
ThisBuild / licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer(
    id = "EtaCassiopeia",
    name = "Mohsen Zainalpour",
    email = "zainalpour@gmail.com",
    url = url("https://github.com/EtaCassiopeia")
  )
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/EtaCassiopeia/zio-dynamodb-codec"),
    "scm:git:git@github.com:EtaCassiopeia/zio-dynamodb-codec.git"
  )
)

ThisBuild / sonatypeCredentialHost := sonatypeCentralHost
ThisBuild / versionScheme          := Some("semver-spec")

lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-Xfatal-warnings",
    "-language:higherKinds"
  )
)

lazy val root = project
  .in(file("."))
  .aggregate(`schema-dynamodb`, `dynamodb-codec-zio`, benchmarks)
  .settings(
    name           := "zio-dynamodb-codec",
    publish / skip := true
  )

val zioVersion      = "2.1.24"
val dynosaurVersion = "0.7.0"

lazy val `schema-dynamodb` = project
  .in(file("schema-dynamodb"))
  .settings(commonSettings)
  .settings(
    name := "zio-blocks-schema-dynamodb",
    libraryDependencies ++= Seq(
      "dev.zio"               %% "zio-blocks-schema" % zioBlocksVersion,
      "software.amazon.awssdk" % "dynamodb"          % awsSdkVersion,
      "dev.zio"               %% "zio-test"          % zioVersion      % Test,
      "dev.zio"               %% "zio-test-sbt"      % zioVersion      % Test,
      "org.systemfw"          %% "dynosaur-core"     % dynosaurVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val `dynamodb-codec-zio` = project
  .in(file("dynamodb-codec-zio"))
  .dependsOn(`schema-dynamodb`)
  .settings(commonSettings)
  .settings(
    name := "zio-dynamodb-codec",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"          % zioVersion,
      "dev.zio" %% "zio-streams"  % zioVersion,
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val benchmarks = project
  .in(file("benchmarks"))
  .enablePlugins(JmhPlugin)
  .dependsOn(`schema-dynamodb`)
  .settings(
    name           := "benchmarks",
    publish / skip := true,
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      "org.systemfw"          %% "dynosaur-core"     % dynosaurVersion,
      "dev.zio"               %% "zio-blocks-schema" % zioBlocksVersion,
      "software.amazon.awssdk" % "dynamodb"          % awsSdkVersion,
      "org.scanamo"           %% "scanamo"           % "1.1.0"
    ),
    scalacOptions ++= Seq("-deprecation", "-feature")
  )
