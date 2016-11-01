package fs2fv

import cats.data.Xor

import com.typesafe.scalalogging.StrictLogging

import filevalidation._
import fs2fv.EnrichNTOps._

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

    // can I use shapeless to construct this coproduct in a less clunky way? Looks like it uses :+: ...
    type F0[A] = Coproduct[StreamOpsDsl, DatabaseOpsDsl, A]
    // apparently the ordering in the following is important, if you switch the first and second type params it
    // fails to compile
    type App[A] = Coproduct[FileValidationDsl, F0, A]

    val prg: Free[App, Int] = runBatchFree[App](requiredFiles, targetDir, forDate, startedAt)

    val streamInt: StreamOpsDsl ~> Task  = new StreamOpsInterpreter(
        StreamOpsParams(targetDir, mkPersistRF, tooManyErrors, 4096, 1000))
    val databaseInt: DatabaseOpsDsl ~> Task = new DatabaseOpsInterpreter(xa)
    val fileValidator = StreamOps.validateFile(targetDir, mkPersistRF, FileValidation.tooManyErrors, 4096, 1000) _
    val validationInt: FileValidationDsl ~> Task  = new FileValidationInterpreter(targetDir, fileValidator)

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
}
