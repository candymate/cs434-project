name := "cs434-project"

version := "1.0.0"
scalaVersion := "2.13.6"

// scalatest
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.9"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % "test"

// junit
libraryDependencies += "junit" % "junit" % "4.13" % "test"

// to use junit with scalatest
libraryDependencies += "org.scalatestplus" %% "junit-4-13" % "3.2.9.0" % "test"