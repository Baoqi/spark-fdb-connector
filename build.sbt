import sbt._
import Dependencies._

lazy val buildSettings = Seq(
  organization         := "com.guandata",
  version in ThisBuild := Versions.currentVersion,
  scalaVersion         := Versions.scalaVersion,
  crossScalaVersions   := Versions.crossScala,
  crossVersion         := CrossVersion.binary
)


lazy val fdbCommon = (project in file("common")).
  settings(
    buildSettings,
    name := "fdb-common",
    libraryDependencies ++= Seq(
      fdbClientDep,
      scalaTest % Test
    )
  )


lazy val root = (project in file(".")).
  settings(
    buildSettings,
    name := "spark-fdb-connector",
    libraryDependencies ++= Seq(
      sparkDep % Provided,
      scalaTest % Test
    )
  ).aggregate(fdbCommon)
  .dependsOn(fdbCommon)
