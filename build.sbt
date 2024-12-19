ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "SPARK_PROJECTS"
  )


libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33"

// Only include spark-sql, not spark-core and spark-streaming separately
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"%"provided"