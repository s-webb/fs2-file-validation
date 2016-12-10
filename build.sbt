name := "fs2fv"

// without the SI-2712 fix, the :+: operator for combining interpreters doesn't work
scalaVersion := "2.11.8"
scalaOrganization in ThisBuild := "org.typelevel"

scalacOptions ++= Seq(
  "-deprecation", 
  "-feature", 
  "-language:higherKinds",
  "-Ypartial-unification"
)

val circeV = "0.5.0-M2"
val doobieV = "0.3.0"
val fs2V = "0.9.1"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.0-M14" % "test",
  "org.typelevel" %% "cats" % "0.6.1",
  "com.chuusai" %% "shapeless" % "2.3.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0",
  "ch.qos.logback" %  "logback-classic" % "1.1.7",
  "org.tpolecat" %% "doobie-core" % doobieV,
  "org.tpolecat" %% "doobie-contrib-postgresql" % doobieV,
  "org.tpolecat" %% "doobie-contrib-h2" % doobieV % "test",
  "org.tpolecat" %% "doobie-contrib-hikari" % doobieV % "test",
  "io.circe" %% "circe-core" % circeV,
  "io.circe" %% "circe-jawn" % circeV,
  "org.spire-math" %% "algebra" % "0.4.0",
  "co.fs2" %% "fs2-core" % fs2V,
  "co.fs2" %% "fs2-io" % fs2V,
  "co.fs2" %% "fs2-scalaz" % "0.1.0",
  "org.scalaz" %% "scalaz-core" % "7.2.6",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
  "junit" % "junit" % "4.12"
)

enablePlugins(JavaAppPackaging)

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.2")
