import sbt._

object Versions {
  val currentVersion = "0.0.1"

  val crossScala = Seq("2.11.12", "2.12.6")

  lazy val scalaVersion = sys.props.get("scala-2.12") match {
    case Some(is) if is.nonEmpty && is.toBoolean => crossScala.last
    case _ => crossScala.head
  }
}

object Dependencies {
  lazy val sparkDep = "org.apache.spark" %% "spark-sql" % "2.3.1"
  lazy val fdbClientDep = "org.foundationdb" % "fdb-java" % "5.2.5"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
}
