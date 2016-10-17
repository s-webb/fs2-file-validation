package fs2fv

import java.nio.file.{Files, Path, Paths}

import java.time.{Duration, LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import scala.collection.JavaConverters._

import cats.data.Xor

import com.typesafe.scalalogging.StrictLogging

import doobie.imports._
import doobie.util.transactor.Transactor

import fs2.{async, concurrent, io, Pipe, Sink, Strategy, Stream}
// need to use scalaz Task rather than fs2 Task for interop with doobie,
// so using the scalaz Task Effect from fs2/scalaz interop module
import fs2.interop.scalaz._

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
// To initialise database
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
//
// ... at something like 90MB/h (8m rows/h) currently!
object Main extends App with StrictLogging {

  run(args(0), args(1), args(2))

  // configFile: ../../../../it/small/config.json
  // targetDateStr: ../../../../it/small/staging/
  // forDateText: 2016-07-21
  def run(configFile: String, targetDirStr: String, forDateText: String): Unit = {
    import FileValidation._
    import FileValidationStreamOps._
    import FileValidationDbOps._

    val startedAt = LocalDateTime.now
    val configText = new String(Files.readAllBytes(Paths.get(configFile)))
    val targetDir = Paths.get(targetDirStr)

    val xa = DbAccess.xa
    val mkPersistRF: Int => PersistRowFailures = id => insertRowFailures(id, xa)
    val eachFile = validateFile(targetDir, mkPersistRF, 4096, 1000) _

    val initJob = initialiseJob(xa) _

    val exitCodeT = FileValidation.runBatch(
      LocalDateTime.now, configText, LocalDate.parse(forDateText), targetDir, initJob, eachFile)
    val exitCode = exitCodeT.unsafePerformSync

    val endedAt = LocalDateTime.now
    val duration = Duration.between(startedAt, endedAt)

    logger.info(s"Run completed in duration of: $duration")
    logger.info(s"Terminating with exit code: $exitCode")

    System.exit(exitCode)
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

  import FileValidationStreamOps._

  implicit val S = fs2.Strategy.fromFixedDaemonPool(4, threadName = "worker")

  type FileValidator = (String, Int) => Stream[Task, ((Int, Int), Seq[String])]
  // I don't like this name!
  type PersistRowFailures = Vector[RowFailure] => Task[Vector[Int]]
  type InitialiseJob = (LocalDateTime, Set[String], Set[String]) => Task[(Int, Map[String, Int])]

  // Still to do...
  // - abstract over Task completely (would need to abstract Task out of StreamOps first)
  def runBatch(startedAt: LocalDateTime, configText: String, forDate: LocalDate, targetDir: Path, 
      initialiseJob: InitialiseJob, fileValidator: FileValidator): Task[Int] = {

    RequiredFile.parseRequiredFiles(configText) match {
      case Xor.Right(required) =>

        val (requiredAt, missing) = checkFilesExist(targetDir, required, forDate)
        val initialisationTask = initialiseJob(startedAt, requiredAt, missing)

        val filesToExitCode: (Int, Map[String, Int]) => Task[Int] = (jobId, filenamesAndIds) => {
          if (missing.size > 0) {
            Task.delay(1) 
          } else {
            val errCountsL: List[Task[Int]] = filenamesAndIds.map { case (name, id) =>
              runSingleFile(name, id, fileValidator, targetDir)
            }.toList
            errCountsL.sequenceU.map(cs => if (cs.exists(_ > 0)) 1 else 0)
          }
        }

        for {
          namesAndIds <- initialisationTask
          exitCode <- filesToExitCode.tupled(namesAndIds)
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

    // the runLast here discards the stream of valid data...
    val rowErrCountTask: Task[(Int, Int)] = 
      fileValidator(name, id).runLast.map(_.map(_._1).getOrElse((0, 0)))

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

  def validateFile(targetDir: Path,
      mkPersistRF: Int => PersistRowFailures,
      fileChunkSizeBytes: Int = 4096, persistBatchSize: Int = 100)
      (filename: String, fileId: Int): Stream[Task, ((Int, Int), Seq[String])] = {

    val bytes: Stream[Task, Byte] = io.file.readAll[Task](targetDir.resolve(filename), fileChunkSizeBytes)
    bytes.through(validateFileBytes(filename, mkPersistRF(fileId), persistBatchSize, tooManyErrors))
  }

  // Below here, is file validation stuff
  def tooManyErrors(f: Int, p: Int): Boolean = {
    val total = f + p
    // where 'too high' is >= 50% after 10000 records
    (total > 10000) && ((f * 100) / total) < 50
  }
}

/**
 * This is a namespace for all stream operations related to file validation
 */
object FileValidationStreamOps {

  import FileValidation.PersistRowFailures
  import RowValidation.validateBytes

  // tooManyErrors should really be an arg here
  def validateFileBytes(filename: String, 
      persistRF: PersistRowFailures, persistBatchSize: Int,
      tooManyErrors: (Int, Int) => Boolean): Pipe[Task, Byte, ((Int, Int), Seq[String])] = bytes => {

    validateBytes(bytes, Some(filename)).
    observe(sinkRowFailures(persistRF, persistBatchSize)).
    through(countsAndPasses).
    takeWhile { case ((f, p), _) => !tooManyErrors(f, p) }
  }

  def countsAndPasses[F[_]]: Pipe[F, Either[RowFailure, Seq[String]], ((Int, Int), Seq[String])] = in => {
    val z: ((Int, Int), Seq[String]) = ((0, 0), Seq[String]())
    val counted = in.scan (z) { 
      case (((f, p), _), Left(failure)) => ((f + 1, p), Seq[String]())
      case (((f, p), _), Right(pass)) => ((f, p + 1), pass)
    }
    counted.filter(_._2.size > 0)
  }

  def sinkRowFailures(persist: PersistRowFailures, 
      persistBatchSize: Int = 1000): Sink[Task, Either[RowFailure, Seq[String]]] = {

    in: Stream[Task, Either[RowFailure, Seq[String]]] => {
      val failures: Stream[Task, RowFailure] = in.collect { case Left(f) => f }
      val failuresChunked: Stream[Task, Vector[RowFailure]] = failures.vectorChunkN(persistBatchSize)

      val errIdsNested: Stream[Task, Stream[Task, Vector[Int]]] = failuresChunked.map(c => Stream.eval(persist(c)))
      concurrent.join(3)(errIdsNested).map(_ => ())
    }
  }
}

/**
 * This is a namespace for all database operations related to file validation
 */
object FileValidationDbOps extends StrictLogging {

  import FileValidation.PersistRowFailures

  def initialiseJob(xa: Transactor[Task])(startedAt: LocalDateTime, requiredAt: Set[String], 
      missing: Set[String]): Task[(Int, Map[String, Int])] = {

    val jobDetails: ConnectionIO[(Int, Map[String, Int])] = 
      for {
        jobId <- DbAccess.insertJob(startedAt)
        filenamesAndIds <- recordFiles(jobId, requiredAt)
        _ <- recordMissing(missing, filenamesAndIds)
      } yield {
        (jobId, filenamesAndIds)
      }

    xa.trans(jobDetails)
  }

  private def recordFiles(jobId: Int, files: Set[String]): ConnectionIO[Map[String, Int]] = {
    val filenamesAndIdsC: List[ConnectionIO[(String, Int)]] = files.toList.map { filename =>
      DbAccess.insertFile(jobId, filename).map(id => (filename, id))
    }
    filenamesAndIdsC.sequenceU.map(_.toMap)
  }

  private def recordMissing(missing: Set[String], 
      filenamesAndIds: Map[String, Int]): ConnectionIO[Set[(String, Int)]] = {

    val missingErrorsC: List[ConnectionIO[(String, Int)]] = missing.toList.map { filename =>
      DbAccess.insertFileError(filenamesAndIds(filename), "missing").map(id => (filename, id))
    }
    missingErrorsC.sequenceU.map(_.toSet)
  }

  def insertRowFailures(fileId: Int, xa: Transactor[Task]): PersistRowFailures = is => {
    val inserts: Vector[ConnectionIO[Int]] = is.map(DbAccess.failureToInsert(fileId)(_))
    val sequenced: ConnectionIO[Vector[Int]] = inserts.sequenceU
    for {
      _ <- Task.delay(logger.info(s"Inserting row error(s), size ${is.size}"))
      ids <- xa.trans(sequenced)
    } yield ids
  }
}
