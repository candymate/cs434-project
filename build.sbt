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
    "org.scalatestplus" %% "junit-4-13" % "3.2.9.0" % "test",
    // scalapb
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
    // grpc
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
    // logging
    "org.slf4j" % "slf4j-api" % "1.7.32",
    "org.slf4j" % "slf4j-simple" % "1.7.32"
)

lazy val root = (project in file("."))
    .settings(
        libraryDependencies ++= commonSettings,
        Compile / PB.targets := Seq(
            scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
        ),
        assembly / assemblyMergeStrategy := {
            case PathList("META-INF", xs @ _*) => MergeStrategy.discard
            case x => MergeStrategy.first
        }
    )

lazy val Master = (project in file("Master"))
    .settings(
        libraryDependencies ++= commonSettings,
        assembly / assemblyJarName := s"${name.value}.jar",
        assembly / assemblyMergeStrategy := {
            case PathList("META-INF", xs @ _*) => MergeStrategy.discard
            case x => MergeStrategy.first
        }
    )
    .dependsOn(root)

lazy val Worker = (project in file("Worker"))
    .settings(
        libraryDependencies ++= commonSettings,
        assembly / assemblyJarName := s"${name.value}.jar",
        assembly / assemblyMergeStrategy := {
            case PathList("META-INF", xs @ _*) => MergeStrategy.discard
            case x => MergeStrategy.first
        }
    )
    .dependsOn(root)
