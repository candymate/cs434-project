name := "cs434-project"
ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.13.6"

lazy val commonSettings = Seq(
    // scalatest
    "org.scalactic" %% "scalactic" % "3.2.9",
    "org.scalatest" %% "scalatest" % "3.2.9" % "test",
    // junit
    "junit" % "junit" % "4.13" % "test",
    // to use junit with scalatest
    "org.scalatestplus" %% "junit-4-13" % "3.2.9.0" % "test"
)

lazy val root = (project in file("."))
    .settings(
        libraryDependencies ++= commonSettings
    )

lazy val Master = (project in file("Master"))
    .settings(
        libraryDependencies ++= commonSettings,
        assembly / assemblyJarName := s"${name.value}.jar"
    )

lazy val Worker = (project in file("Worker"))
    .settings(
        libraryDependencies ++= commonSettings,
        assembly / assemblyJarName := s"${name.value}.jar"
    )
