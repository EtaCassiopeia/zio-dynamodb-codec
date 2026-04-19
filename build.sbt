val scala3Version    = "3.3.6"
val zioBlocksVersion = "0.0.33"
val awsSdkVersion    = "2.31.61"

ThisBuild / organization := "dev.zio"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := scala3Version

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
  .aggregate(`schema-dynamodb`, `dynamodb-codec-zio`)
  .settings(
    name         := "zio-dynamodb-codec",
    publish      := {},
    publishLocal := {}
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
