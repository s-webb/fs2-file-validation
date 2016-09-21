package fs2fv

import java.nio.file.{Files, Path, Paths}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.collection.JavaConverters._

import cats.data.Xor

import com.typesafe.scalalogging.StrictLogging

import doobie.imports._
import doobie.util.transactor.Transactor

import fs2.{async, concurrent, io, Strategy, Stream}
// need to use scalaz Task rather than fs2 Task for interop with doobie,
// so using the scalaz Task Effect from fs2/scalaz interop module
import fs2.interop.scalaz._

import scalaz._, Scalaz._
import scalaz.concurrent.Task

object Main extends App with StrictLogging {

  val configFile = args(0)
  val targetDirStr = args(1)
  val forDateText = args(2)

  val configText = new String(Files.readAllBytes(Paths.get(configFile)))
  val targetDir = Paths.get(targetDirStr)

  val xa = DbAccess.xa
  val eachFile = FileValidation.validateFile(targetDir, xa, 4096, 100) _
  val exitCode = FileValidation.run(
    LocalDate.now, configText, LocalDate.parse(forDateText), targetDir, xa, eachFile)

  logger.info(s"Terminating with exit code: $exitCode")
  System.exit(exitCode)
}

/**
 * Things to do now...
 *  - use a signal to stop processing if error count rises above acceptable limit
 *  - merge records from X sorted files concurrently
 */
object FileValidation extends StrictLogging {

  implicit val S = fs2.Strategy.fromFixedDaemonPool(4, threadName = "worker")

  type FileValidator = (String, Int) => Stream[Task, Int]

  def run(startedAt: LocalDate, configText: String, forDate: LocalDate, targetDir: Path, 
      xa: Transactor[Task], fileValidator: FileValidator): Int = {

    RequiredFile.parseRequiredFiles(configText) match {
      case Xor.Right(required) =>

        logger.info(s"Checking for files in ${targetDir.toFile.getAbsolutePath}")
        val found = targetDir.toFile.list.toSet
        logger.info(s"Found ${found.size} file(s) in staging dir")
        found.foreach { f =>
          logger.debug(s"Found file $f")
        }
        val requiredAt = requiredForDate(required, forDate)

        val missing = requiredAt -- found

        val initialisationTask = initialiseFiles(xa, startedAt, requiredAt, missing)
        val (jobId, filenamesAndIds) = initialisationTask.unsafePerformSync

        if (missing.size > 0) 1 else {
          // filenamesAndIds could be a Stream too...
          val filenamesAndIdsS: Stream[Nothing, (String, Int)] = Stream(filenamesAndIds.toList:_*)
          val errIdsNested: Stream[Task, Stream[Task, Int]] = filenamesAndIdsS.map(fileValidator.tupled)
          val errIds: Stream[Task, Int] = concurrent.join(2)(errIdsNested)

          val rowErrCountTask: Task[Int] = errIds.zipWithIndex.runLast.map(_.map(_._2 + 1).getOrElse(0))
          val rowErrCount: Int = rowErrCountTask.unsafePerformSync
          logger.info(s"Count of row errors: ${rowErrCount}")

          if (rowErrCount > 0) 1 else 0
        }

      case Xor.Left(error) =>
        logger.error(s"Failed to parse config file")
        1
    }
  }

  def initialiseFiles(xa: Transactor[Task], startedAt: LocalDate, requiredAt: Set[String], 
      missing: Set[String]): Task[(Int, Map[String, Int])] = {

    val missingC: ConnectionIO[(Int, Map[String, Int])] = 
      for {
        jobId <- newJobId(startedAt)
        filenamesAndIds <- recordFiles(jobId, requiredAt)
        _ <- recordMissing(missing, filenamesAndIds)
      } yield {
        (jobId, filenamesAndIds)
      }
    xa.trans(missingC)
  }

  def validateFile(targetDir: Path, xa: Transactor[Task], fileChunkSizeBytes: Int = 4096, insertBatchSize: Int = 100)
      (filename: String, fileId: Int): Stream[Task, Int] = {

    // A reasonable chunk size to read might be a few kb, going for a really small value to see
    // if it has any impact on concurrency
    val bytes: Stream[Task, Byte] = io.file.readAll[Task](targetDir.resolve(filename), fileChunkSizeBytes)
    val toInsert = DbAccess.failureToInsert(fileId) _

    val failures: Stream[Task, RowFailure] = RowValidation.validateBytes(bytes, Some(filename)).collect {
      case Left(f) => f
    }
    val inserts: Stream[Task, ConnectionIO[Int]] = failures.map(toInsert)
    val insertsB: Stream[Task, Vector[ConnectionIO[Int]]] = inserts.vectorChunkN(insertBatchSize)

    val errIdsNested: Stream[Task, Stream[Task, Int]] = insertsB.map { is => 
      val doLog = Task.delay(logger.debug(s"Inserting row error(s), size ${is.size}"))
      val t: Task[Stream[Nothing, Int]] = doLog.flatMap(_ => xa.trans(is.sequenceU).map { ids => 
        Stream(ids:_*) 
      })
      Stream.eval(t).flatMap(x => x)
    }
    val errIds: Stream[Task, Int] = concurrent.join(3)(errIdsNested)

    val controlled: Stream[Task, Int] = Stream.eval(async.signalOf[Task, Int](0)).flatMap { errCountSignal =>
      val errorsAboveThreshold = errCountSignal.discrete.map(_ > 10)
      val errsUpdateCount = errIds.zipWithIndex.evalMap { case (id, count) =>
        errCountSignal.set(count).map(_ => id)
      }
      errsUpdateCount interruptWhen errorsAboveThreshold
    }

    controlled
  }

  def newJobId(startedAt: LocalDate): ConnectionIO[Int] = {
    DbAccess.insertJob(startedAt)
  }

  def recordFiles(jobId: Int, files: Set[String]): ConnectionIO[Map[String, Int]] = {
    val filenamesAndIdsC: List[ConnectionIO[(String, Int)]] = files.toList.map { filename =>
      DbAccess.insertFile(jobId, filename).map(id => (filename, id))
    }
    filenamesAndIdsC.sequenceU.map(_.toMap)
  }

  def recordMissing(missing: Set[String], filenamesAndIds: Map[String, Int]): ConnectionIO[Set[(String, Int)]] = {
    val missingErrorsC: List[ConnectionIO[(String, Int)]] = missing.toList.map { filename =>
      logger.info(s"Missing file $filename")
      DbAccess.insertFileError(filenamesAndIds(filename), "missing").map(id => (filename, id))
    }
    missingErrorsC.sequenceU.map(_.toSet)
  }

  def requiredForDate(required: Set[RequiredFile], forDate: LocalDate): Set[String] =
    required.map(_.nameForDate(forDate))
}
