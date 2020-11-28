ThisBuild / organization := "org.charleso"
ThisBuild / name := "fuse"
ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.12"

val sparkVersion  = "2.4.7"

ThisBuild / libraryDependencies ++= List(
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided
  , "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided
  , "qa.hedgehog"  %% "hedgehog-sbt" % "0.5.1" % Test
  )

ThisBuild / testFrameworks := Seq(TestFramework("hedgehog.sbt.Framework"))

lazy val fuse = (project in file("."))
  .aggregate(scalaz)

lazy val scalaz = (project in file("scalaz"))
  .settings(
    libraryDependencies ++= Seq(
      "org.scalaz" %% "scalaz-core" % "7.2.23"
    )
  )
