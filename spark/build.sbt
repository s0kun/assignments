ThisBuild / scalaVersion := "2.13.0"

// javacOptions ++= Seq("-source", "21", "-target", "21")

lazy val covidAPI = project.in(file("."))
  .settings(
    name := "covidAnalysis",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "com.softwaremill.sttp.client4" %% "core" % "4.0.0-M11",
      "com.lihaoyi" %% "upickle" % "3.1.4"
    )
  )