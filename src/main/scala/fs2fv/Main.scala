package fs2fv

import fs2.interop.scalaz._

import fs2fv.ValidateAndMerge.{validateAndMerge, RowValidator}
import fs2fv.RowValidator.rowValidator

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import java.io.File
import java.nio.file.{Files, Path, Paths}

import scalaz.concurrent.Task

object Main extends App {

  println("Hit enter to begin")
  System.console.readLine
  run(args(0))

  def run(workingDir: String) {
    val dir = Paths.get(workingDir)
    val configStr = new String(Files.readAllBytes(dir.resolve("config.json")))
    val configE: Either[Error, Config] = decode[Config](configStr)

    // given a config, how do I use the thing
    configE match { 
      case Right(Config(outputs)) => 
        outputs.map(o => (generate(dir, o), o)).foreach { case (g, o) =>
          val before = System.currentTimeMillis
          val errCounts = g.unsafePerformSync
          errCounts.zip(o.inputs.map(_.name)).foreach { case (e, i) =>
            println(s"Inbound file $i had $e errors")
          }
          val after = System.currentTimeMillis
          println(s"Took ${after - before} millis")
          println("Hit enter to end")
          System.console.readLine
        }
      case Left(err) => throw new RuntimeException("fail")
    }
  }

  def clean(workingDir: String) {
    val dir = Paths.get(workingDir)
    val out = dir.resolve("output")
    deleteRecursively(out.toFile)
    val rejects = dir.resolve("rejects")
    deleteRecursively(rejects.toFile)
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete) {
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")   
    }
  }

  def generate(workingDir: Path, conf: Output): Task[Seq[Int]] = {
    val in: Seq[(Path, RowValidator[Task])] = conf.inputs.map { i => 
      val p = workingDir.resolve("staging/" + i.name)
      val rv = rowValidator[Task](i)
      (p, rv)
    }
    val out = workingDir.resolve("output/" + conf.name)
    out.toFile.getParentFile.mkdirs()
    val rejects = workingDir.resolve("rejects")
    rejects.toFile.mkdirs()
    val countsT: Task[Vector[Seq[Int]]] = validateAndMerge(in, out, rejects).runLog
    countsT.map(_(0))
  }
}
