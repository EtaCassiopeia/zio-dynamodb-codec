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
  .aggregate(`schema-dynamodb`)
  .settings(
    name         := "zio-dynamodb-codec",
    publish      := {},
    publishLocal := {}
  )

lazy val `schema-dynamodb` = project
  .in(file("schema-dynamodb"))
  .settings(commonSettings)
  .settings(
    name := "zio-blocks-schema-dynamodb",
    libraryDependencies ++= Seq(
      "dev.zio"               %% "zio-blocks-schema" % zioBlocksVersion,
      "software.amazon.awssdk" % "dynamodb"          % awsSdkVersion
    )
  )
