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
    val startedAt = LocalDateTime.now
    val configText = new String(Files.readAllBytes(Paths.get(configFile)))
    val targetDir = Paths.get(targetDirStr)

    val xa = DbAccess.xa
    val eachFile = FileValidation.validateFile(targetDir, xa, 4096, 100) _
    val exitCode = FileValidation.run(
      LocalDateTime.now, configText, LocalDate.parse(forDateText), targetDir, xa, eachFile)

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
 */
object FileValidation extends StrictLogging {

  implicit val S = fs2.Strategy.fromFixedDaemonPool(4, threadName = "worker")

  type FileValidator = (String, Int) => Stream[Task, ((Int, Int), Seq[String])]

  def run(startedAt: LocalDateTime, configText: String, forDate: LocalDate, targetDir: Path, 
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


          val errCounts: Iterable[Int] = filenamesAndIds.map { case (name, id) =>

            val startedAt = LocalDateTime.now

            val countsAndData: Stream[Task, (((Int, Int), Seq[String]))] = fileValidator(name, id)
            val rowErrCountTask: Task[(Int, Int)] = countsAndData.runLast.map(_.map(_._1).getOrElse((0, 0)))
            val (rowErrCount, rowPassCount): (Int, Int) = rowErrCountTask.unsafePerformSync

            val endedAt = LocalDateTime.now
            val duration = Duration.between(startedAt, endedAt)
            val numSeconds = duration.getSeconds

            val file = targetDir.resolve(name).toFile
            val numBytes = file.length()
            val numMb = numBytes / (1024L * 1024L)
            val mbPerS = numMb.toDouble / numSeconds.toDouble

            logger.info(f"Finished processing $name, size: ${numMb}MB, seconds: $numSeconds ($mbPerS%2.2f MB/s)")
            logger.info(s"Total rows: ${rowErrCount + rowPassCount}, failed: ${rowErrCount}, passed: ${rowPassCount}")
            rowErrCount
          }
          if (errCounts.exists(_ > 0)) 1 else 0

          // // filenamesAndIds could be a Stream too...
          // val filenamesAndIdsS: Stream[Nothing, (String, Int)] = Stream(filenamesAndIds.toList:_*)
          // val passAndCountNested: Stream[Task, Stream[Task, ((Int, Int), Seq[String])]] = 
          //   filenamesAndIdsS.map(fileValidator.tupled)

          // // at this point I've flattened the contents of all of the files into a single stream,
          // // which is not what I want!
          // val passAndCount: Stream[Task, ((Int, Int), Seq[String])] = concurrent.join(4)(passAndCountNested)

          // // all of the good data is just being dumped at this point, really I need to 
          // // pipe it into a merge
          // val rowErrCountTask: Task[(Int, Int)] = passAndCount.runLast.map(_.map(_._1).getOrElse((0, 0)))
          // val (rowErrCount, rowPassCount): (Int, Int) = rowErrCountTask.unsafePerformSync
          // logger.info(s"Processed ${rowErrCount + rowPassCount} rows, ${rowErrCount} failures, ${rowPassCount} passes")

          // if (rowErrCount > 0) 1 else 0
        }

      case Xor.Left(error) =>
        logger.error(s"Failed to parse config file")
        1
    }
  }

  def initialiseFiles(xa: Transactor[Task], startedAt: LocalDateTime, requiredAt: Set[String], 
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

  // TODO Would like to refactor this into a Pipe, from Stream[F, Byte] to Stream[F, (Int, Seq[String])]
  // should be simple enough
  def validateFile(targetDir: Path, xa: Transactor[Task], fileChunkSizeBytes: Int = 4096, insertBatchSize: Int = 100)
      (filename: String, fileId: Int): Stream[Task, ((Int, Int), Seq[String])] = {

    // A reasonable chunk size to read might be a few kb, going for a really small value to see
    // if it has any impact on concurrency
    val bytes: Stream[Task, Byte] = io.file.readAll[Task](targetDir.resolve(filename), fileChunkSizeBytes)

    val passesAndFailures: Stream[Task, Either[RowFailure, Seq[String]]] = 
      RowValidation.validateBytes(bytes, Some(filename))

    // rowErrorsSink will insert failures into DB
    val observed = passesAndFailures.observe(rowErrorsSink(fileId, xa, insertBatchSize))
    // count and passes will accumulate an error count along with the stream of good data
    val passesAndCount: Stream[Task, ((Int, Int), Seq[String])] = observed.through(countAndPasses)

    // instead of using a signal, can I just takeWhile?
    passesAndCount.takeWhile { case ((f, _), _) => f <= 10000 }
  }

  def countAndPasses[F[_]]: Pipe[F, Either[RowFailure, Seq[String]], ((Int, Int), Seq[String])] = in => {
    val z: ((Int, Int), Seq[String]) = ((0, 0), Seq[String]())
    val counted = in.scan (z) { 
      case (((failCount, passCount), _), Left(failure)) => ((failCount + 1, passCount), Seq[String]())
      case (((failCount, passCount), _), Right(pass)) => ((failCount, passCount + 1), pass)
    }
    counted.filter(_._2.size > 0)
  }

  def rowErrorsSink(fileId: Int, xa: Transactor[Task], 
      insertBatchSize: Int = 100): Sink[Task, Either[RowFailure, Seq[String]]] = { 

    in: Stream[Task, Either[RowFailure, Seq[String]]] => {
      val failures: Stream[Task, RowFailure] = in.collect {
        case Left(f) => f
      }

      val toInsert = DbAccess.failureToInsert(fileId) _
      val inserts: Stream[Task, ConnectionIO[Int]] = failures.map(toInsert)
      val insertsB: Stream[Task, Vector[ConnectionIO[Int]]] = inserts.vectorChunkN(insertBatchSize)

      val errIdsNested: Stream[Task, Stream[Task, Int]] = insertsB.map { is => 
        val doLog = Task.delay(logger.info(s"Inserting row error(s), size ${is.size}"))
        val t: Task[Stream[Nothing, Int]] = doLog.flatMap(_ => xa.trans(is.sequenceU).map { ids => 
          Stream(ids:_*) 
        })
        Stream.eval(t).flatMap(x => x)
      }
      concurrent.join(6)(errIdsNested).map(_ => ())
    }
  }

  def newJobId(startedAt: LocalDateTime): ConnectionIO[Int] = {
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
