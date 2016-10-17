name := "fs2fv"

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-deprecation", 
  "-feature", 
  "-language:higherKinds"
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
  "co.fs2" %% "fs2-scalaz" % "0.1.0"
)

enablePlugins(JavaAppPackaging)
