package fs2fv

import cats.data.Xor

import com.typesafe.scalalogging.StrictLogging

import fs2fv.EnrichNTOps._
import FileValidation.PersistRowFailures
import filevalidation._
import fs2fv.filevalidation.StreamOps

import java.nio.file.{Files, Path, Paths}

import java.time.{Duration, LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import scala.collection.JavaConverters._

import scalaz._, Scalaz._
import scalaz.concurrent.Task

// Run with these args...
//
// fs2fv ../../../../it/small/config.json ../../../../it/small/staging/ 2016-07-21
//
// Or...
//
// fs2fv ../../../../it/large/config.json ../../../../it/large/staging/ 2016-07-21
//
// To initialise database:
//
// create database fs2fv;
// create role fs2fv_user with login password 'password';
// \i /home/simonpw/Documents/workspace/fs2-file-validation/src/main/resources/fs2fv.ddl.sql
//
// Seems to take 140s to process 200,000 rows of data (with error rate of 5%, though I doubt that matters)
// So, 1MB per minute, ish. Rubbish.
//
// ... and yet I can process 1,000,000 rows in 16s if I don't use the control signal. Hmm.
// 10MB (ish) is 15s, 40MB/min, 2400MB/h (240m rows/h)
object Main extends App with StrictLogging {

  run(args(0), args(1), args(2))

  // configFile: ../../../../it/small/config.json
  // targetDateStr: ../../../../it/small/staging/
  // forDateText: 2016-07-21
  def run(configFile: String, targetDirStr: String, forDateText: String): Unit = {
    import FileValidation._
    import filevalidation.StreamOps._
    import filevalidation.DatabaseOps._

    val startedAt = LocalDateTime.now
    val configText = new String(Files.readAllBytes(Paths.get(configFile)))
    val requiredFiles = 
      RequiredFile.parseRequiredFiles(configText) match {
        case Xor.Right(required) => required
        case Xor.Left(e) => throw new RuntimeException("Failed to parse required files", e)
      }
    val targetDir = Paths.get(targetDirStr)

    val xa = DbAccess.xa
    val mkPersistRF: Int => PersistRowFailures = id => insertRowFailures(id, xa)

    val initJob = initialiseJob(xa) _

    val forDate = LocalDate.parse(forDateText)
    // val exitCodeT = FileValidation.runBatch(startedAt, configText, forDate, targetDir, initJob, mkPersistRF)
    // val exitCode = exitCodeT.unsafePerformSync
    

    type F0[A] = Coproduct[StreamOpsDsl, DatabaseOpsDsl, A]
    // apparently the ordering in the following is important, if you switch the first and second type params it
    // fails to compile
    type App[A] = Coproduct[FileValidationDsl, F0, A]

    val prg: Free[App, Int] = FileValidation.runBatchFree[App](requiredFiles, targetDir, forDate, startedAt)

    val streamInt: StreamOpsDsl ~> Task  = new StreamOpsInterpreter(
        StreamOpsParams(targetDir, mkPersistRF, tooManyErrors, 4096, 1000))
    val databaseInt: DatabaseOpsDsl ~> Task = new DatabaseOpsInterpreter(xa)
    val validationInt: FileValidationDsl ~> Task  = new FileValidationInterpreter(targetDir, mkPersistRF)

    // order in which interpreters are composed is signficant, must match that used in building the
    // coproduct
    val interpreter: App ~> Task = validationInt :+: streamInt :+: databaseInt
    val exitCodeT: Task[Int] = prg.foldMap(interpreter)
    val exitCode = exitCodeT.unsafePerformSync

    val endedAt = LocalDateTime.now
    val duration = Duration.between(startedAt, endedAt)

    logger.info(s"Run completed in duration of: $duration")
    logger.info(s"Terminating with exit code: $exitCode")

    System.exit(exitCode)
  }
}

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

class FileValidationInterpreter(targetDir: Path, mkPersistRF: Int => PersistRowFailures) 
    extends (FileValidationDsl ~> Task) {

  val fileValidator = StreamOps.validateFile(targetDir, mkPersistRF, FileValidation.tooManyErrors, 4096, 1000) _

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
 * Things to do now...
 *  - use a signal to stop processing if error count rises above acceptable limit
 *  - merge records from X sorted files concurrently
 *
 *  This part should know nothing about the database.
 */
object FileValidation extends StrictLogging {

  implicit val S = fs2.Strategy.fromFixedDaemonPool(4, threadName = "worker")

  type ErrorThreshold = (Int, Int) => Boolean
  type FileValidator = (String, Int) => Task[(Int, Int)]
  // I don't like this name!
  type PersistRowFailures = Vector[RowFailure] => Task[Vector[Int]]
  type InitialiseJob = (LocalDateTime, Set[String], Set[String]) => Task[(Int, Map[String, Int])]

  import filevalidation.{StreamOpsFree, DatabaseOpsFree}

  def runBatchFree[S[_]](required: Set[RequiredFile], targetDir: Path, forDate: LocalDate, 
    startedAt: LocalDateTime)(implicit
    valid: FileValidationOpsFree.Ops[S],
    stream: StreamOpsFree.Ops[S],
    db: DatabaseOpsFree.Ops[S]
  ): Free[S, Int] = {

    // can't use a destructuring bind in the for statement because free lacks withFilter, because reasons
    // https://github.com/scalaz/scalaz/pull/728
    for {
      r1 <- valid.checkFilesExist(targetDir, required, forDate)
      (requiredAt, missing) = r1
      r2 <- db.initialiseJob(startedAt, requiredAt, missing)
      (jobId, filenamesAndIds) = r2
      exitCode <- valid.validateFiles(jobId, missing, filenamesAndIds)
    } yield exitCode
  }

  def runBatch(startedAt: LocalDateTime, configText: String, forDate: LocalDate, targetDir: Path, 
      initialiseJob: InitialiseJob, mkPersistRF: Int => PersistRowFailures): Task[Int] = {

    RequiredFile.parseRequiredFiles(configText) match {
      case Xor.Right(required) =>


        val (requiredAt, missing) = checkFilesExist(targetDir, required, forDate)
        val initialisationTask = initialiseJob(startedAt, requiredAt, missing)

        val fileValidator = StreamOps.validateFile(targetDir, mkPersistRF, tooManyErrors, 4096, 1000) _
        val filesToExitCode: (Int, Map[String, Int]) => Task[Int] = (jobId, filenamesAndIds) => {
          if (missing.size > 0) {
            1.point[Task]
          } else {
            val errCountsL: List[Task[Int]] = filenamesAndIds.map { case (name, id) =>
              runSingleFile(name, id, fileValidator, targetDir)
            }.toList
            errCountsL.sequenceU.map(cs => if (cs.exists(_ > 0)) 1 else 0)
          }
        }

        for {
          namesAndIds <- initialisationTask
          (jobId, filenamesAndIds) = namesAndIds
          exitCode <- filesToExitCode(jobId, filenamesAndIds)
        } yield exitCode

      case Xor.Left(error) =>
        Task.delay {
          logger.error(s"Failed to parse config file")
          1
        }
    }
  }

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
