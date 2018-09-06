import sbt._
import Dependencies._

lazy val buildSettings = Seq(
  organization         := "com.guandata",
  version in ThisBuild := Versions.currentVersion,
  scalaVersion         := Versions.scalaVersion,
  crossScalaVersions   := Versions.crossScala,
  crossVersion         := CrossVersion.binary,
  scalacOptions ++= Seq("-feature", "-deprecation", "-Xexperimental"),
  publishTo := {
    val nexus = "https://app.mayidata.com/nexus/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "repository/maven-snapshots")
    else
      Some("releases"  at nexus + "repository/maven-releases")
  },
  credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
)


lazy val fdbCommon = (project in file("common")).
  settings(
    buildSettings,
    name := "fdb-common",
    libraryDependencies ++= Seq(
      fdbClientDep,
      rocksDbDep % Provided,
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
