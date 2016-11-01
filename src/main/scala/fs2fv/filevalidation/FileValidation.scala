package fs2fv.filevalidation

import com.typesafe.scalalogging.StrictLogging

import fs2fv.RequiredFile

import java.nio.file.Path

import java.time.{Duration, LocalDate, LocalDateTime}

import scalaz._, Scalaz._
import scalaz.concurrent.Task

sealed trait FileValidationDsl[A]

case class CheckFilesExist(targetDir: Path, required: Set[RequiredFile], 
  forDate: LocalDate) extends FileValidationDsl[(Set[String], Set[String])]

case class ValidateFiles(jobId: Int, missing: Set[String], filenamesAndIds: Map[String, Int]) 
  extends FileValidationDsl[Int]

object FileValidationOpsFree {

  class Ops[S[_]](implicit s0: FileValidationDsl :<: S) {

    def checkFilesExist(targetDir: Path, required: Set[RequiredFile], 
      forDate: LocalDate): Free[S, (Set[String], Set[String])] =
        Free.liftF(s0.inj(CheckFilesExist(targetDir, required, forDate)))

    def validateFiles(jobId: Int, missing: Set[String], filenamesAndIds: Map[String, Int]): Free[S, Int] =
      Free.liftF(s0.inj(ValidateFiles(jobId, missing, filenamesAndIds)))
  }

  object Ops {
    implicit def apply[S[_]](implicit s0: FileValidationDsl :<: S): Ops[S] = {
      new Ops[S]
    }
  }
}

/**
 * Not really happy that FileValidator is being passed here as a dependency.
 * Would seem that this should be done as vertical composition of Frees
 */
class FileValidationInterpreter(targetDir: Path, fileValidator: FileValidator) 
    extends (FileValidationDsl ~> Task) {

  def apply[A](dsl: FileValidationDsl[A]): Task[A] = dsl match {

    case CheckFilesExist(targetDir, required, forDate) =>
      FileValidation.checkFilesExist(targetDir, required, forDate).point[Task]

    case ValidateFiles(jobId, missing, filenamesAndIds) =>
      if (missing.size > 0) {
        1.point[Task]
      } else {
        val errCountsL: List[Task[Int]] = filenamesAndIds.map { case (name, id) =>
          FileValidation.runSingleFile(name, id, fileValidator, targetDir)
        }.toList
        errCountsL.sequenceU.map(cs => if (cs.exists(_ > 0)) 1 else 0)
      }
  }
}

/**
 * Operations for validating a batch of files.
 *
 * I think this needs splitting, so that the composition of free algebras (run batch) and the file-group-specific
 * operations are in separate modules.
 */
object FileValidation extends StrictLogging {

  /**
   * Apply the given FileValidator to the specified file, log performance stats and return the error count
   */
  def runSingleFile(name: String, id: Int, fileValidator: FileValidator, targetDir: Path): Task[Int] = {

    def logStats(startedAt: LocalDateTime, rowErrCount: Int, rowPassCount: Int): Task[Unit] = Task.delay {
      val endedAt = LocalDateTime.now

      val file = targetDir.resolve(name).toFile
      val (durationSeconds, sizeMb, mbPerS) = performanceStats(file.length, startedAt, endedAt)

      logger.info(f"Finished processing $name, size: ${sizeMb}MB, " + 
        f"seconds: $durationSeconds%2.2f ($mbPerS%2.2f MB/s)")
      logger.info(s"Total rows: ${rowErrCount + rowPassCount}, failed: ${rowErrCount}, passed: ${rowPassCount}")
    }

    val rowErrCountTask: Task[(Int, Int)] = fileValidator(name, id)

    for {
      startedAt <- Task.delay(LocalDateTime.now)
      counts <- rowErrCountTask
      _ <- logStats(startedAt, counts._1, counts._2)
    } yield counts._1
  }

  def performanceStats(sizeInBytes: Long, start: LocalDateTime, end: LocalDateTime): (Double, Int, Double) = {
    val duration = Duration.between(start, end)
    val durationSeconds = (duration.toMillis.toDouble / 1000.0d)
    val sizeMb = (sizeInBytes / (1024L * 1024L)).toInt
    val mbPerS = (sizeMb.toDouble / durationSeconds)
    (durationSeconds, sizeMb, mbPerS)
  }

  def checkFilesExist(targetDir: Path, required: Set[RequiredFile], forDate: LocalDate): 
      (Set[String], Set[String]) = {

    logger.info(s"Checking for files in ${targetDir.toFile.getAbsolutePath}")
    val found = targetDir.toFile.list.toSet
    logger.info(s"Found ${found.size} file(s) in staging dir")
    found.foreach { f =>
      logger.debug(s"Found file $f")
    }

    val requiredAt = required.map(_.nameForDate(forDate))
    val missing = requiredAt -- found
    (requiredAt, missing)
  }

  def tooManyErrors(f: Int, p: Int): Boolean = {
    val total = f + p
    // where 'too high' is >= 50% after 10000 records
    (total > 10000) && ((f * 100) / total) > 50
  }
}
