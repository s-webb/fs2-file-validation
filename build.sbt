lazy val Benchmark = config("bench") extend Test

val circeV = "0.6.1"
val doobieV = "0.3.0a"
val fs2V = "0.9.2"

enablePlugins(JavaAppPackaging)

lazy val root = project.in(file(".")).
  configs(Benchmark).
  settings(
    name := "fs2fv",

    // without the SI-2712 fix, the :+: operator for combining interpreters doesn't work
    scalaVersion := "2.12.1",
    // scalaVersion := "2.11.8",
    // scalaOrganization in ThisBuild := "org.typelevel",

    scalacOptions ++= Seq(
      "-deprecation", 
      "-feature", 
      "-language:higherKinds",
      "-Ypartial-unification"
    ),

    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "org.typelevel" %% "cats" % "0.8.1",
      "com.chuusai" %% "shapeless" % "2.3.2",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
      "ch.qos.logback" %  "logback-classic" % "1.1.7",
      "org.tpolecat" %% "doobie-core" % doobieV,
      "org.tpolecat" %% "doobie-contrib-postgresql" % doobieV,
      "org.tpolecat" %% "doobie-contrib-h2" % doobieV % "test",
      "org.tpolecat" %% "doobie-contrib-hikari" % doobieV % "test",
      "io.circe" %% "circe-core" % circeV,
      "io.circe" %% "circe-generic" % circeV,
      "io.circe" %% "circe-jawn" % circeV,
      "io.circe" %% "circe-parser" % circeV,
      // "org.spire-math" %% "algebra" % "0.13.0",
      "co.fs2" %% "fs2-core" % fs2V,
      "co.fs2" %% "fs2-io" % fs2V,
      "co.fs2" %% "fs2-scalaz" % "0.2.0",
      "org.scalaz" %% "scalaz-core" % "7.2.6",
      "com.storm-enroute" %% "scalameter" % "0.8.2" % "bench"
    ),

    addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.3"),

    testFrameworks += new TestFramework(
      "org.scalameter.ScalaMeterFramework"
    ),
      
    logBuffered := false,

    parallelExecution in Benchmark := false
  ).
  settings(inConfig(Benchmark)(Defaults.testSettings):_*)
