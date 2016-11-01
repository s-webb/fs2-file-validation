package fs2fv

import cats.data.Xor

import com.typesafe.scalalogging.StrictLogging

import doobie.util.transactor.Transactor

import filevalidation._
import fs2fv.EnrichNTOps._

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.time.{Duration, LocalDate, LocalDateTime}

import scalaz.concurrent.Task
import scalaz._, Scalaz._

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
    val targetDir = Paths.get(targetDirStr)

    val xa = DbAccess.xa
    val mkPersistRF: Int => PersistRowFailures = id => insertRowFailures(id, xa)
    val fileValidator = StreamOps.validateFile(targetDir, mkPersistRF, FileValidation.tooManyErrors, 4096, 1000) _
    val forDate = LocalDate.parse(forDateText)

    val exitCode = run(startedAt, configText, forDate, targetDir, xa, fileValidator)
    System.exit(exitCode)
  }

  // result type here should really be ParseError \/ Int
  def run(startedAt: LocalDateTime, configText: String, forDate: LocalDate, targetDir: Path, 
      xa: Transactor[Task], fileValidator: FileValidator): Int = {

    val requiredFiles = 
      RequiredFile.parseRequiredFiles(configText) match {
        case Xor.Right(required) => required
        case Xor.Left(e) => throw new RuntimeException("Failed to parse required files", e)
      }

    type App[A] = Coproduct[FileValidationDsl, DatabaseOpsDsl, A]

    val prg: Free[App, Int] = runBatchFree[App](requiredFiles, targetDir, forDate, startedAt)

    val databaseInt: DatabaseOpsDsl ~> Task = new DatabaseOpsInterpreter(xa)
    val validationInt: FileValidationDsl ~> Task  = new FileValidationInterpreter(targetDir, fileValidator)

    val interpreter: App ~> Task = validationInt :+: databaseInt
    val exitCodeT: Task[Int] = prg.foldMap(interpreter)
    val exitCode = exitCodeT.unsafePerformSync

    val endedAt = LocalDateTime.now
    val duration = Duration.between(startedAt, endedAt)

    logger.info(s"Run completed in duration of: $duration")
    logger.info(s"Terminating with exit code: $exitCode")

    exitCode
  }

  def runBatchFree[S[_]](required: Set[RequiredFile], targetDir: Path, forDate: LocalDate, 
    startedAt: LocalDateTime)(implicit
    valid: FileValidationOpsFree.Ops[S],
    db: DatabaseOpsFree.Ops[S]
  ): Free[S, Int] = {

    // can't use a destructuring bind in the for statement because free lacks withFilter, because reasons
    // https://github.com/scalaz/scalaz/pull/728
    def lsTargetDir(): Set[String] = {
      logger.info(s"Checking for files in ${targetDir.toFile.getAbsolutePath}")
      targetDir.toFile.list.toSet
    }
    for {
      r1 <- valid.checkFilesExist(lsTargetDir, required, forDate)
      (requiredAt, missing) = r1
      r2 <- db.initialiseJob(startedAt, requiredAt, missing)
      (jobId, filenamesAndIds) = r2
      exitCode <- valid.validateFiles(jobId, missing, filenamesAndIds)
    } yield exitCode
  }
}
