package fs2fv

import fs2._
import fs2.interop.scalaz._
import fs2fv.filevalidation._

import java.nio.file.{Files, Paths}
import java.time.{LocalDate, LocalDateTime}

import doobie.imports._

import org.scalatest.{Matchers, WordSpecLike}


class MainSpec extends WordSpecLike with Matchers with InitialiseDb {

  override def dbName: String = "file-validation"

  "run" should {
    "return 1 for unsuccessful run" ignore {
      // What dependencies would I like to be able to sub in?
      //  - the actual checks to run
      //  - the database to store the results in
      
      val startedAt = LocalDateTime.parse("2016-08-06T00:00:00")
      val forDate = LocalDate.parse("2016-07-21")
      val targetDir = Paths.get("it/small/staging")
      val configText = new String(Files.readAllBytes(Paths.get("it/small/config.json")))
      val xa = initialiseDb()

      val mkPersistRF: Int => PersistRowFailures = id => DatabaseOps.insertRowFailures(id, xa)
      val eachFile = StreamOps.validateFile(targetDir, mkPersistRF, FileValidation.tooManyErrors, 16, 2) _

      val rslt = Main.run(startedAt, configText, forDate, targetDir, xa, eachFile)
      rslt should be (1)

      val dbRows = sql"""
          SELECT r.id, f.file_id, r.row_num, r.message 
          FROM dq_job_row_error r, dq_job_file f 
          WHERE r.job_file_id = f.id
        """.query[(Int, String, Int, String)].list.transact(xa).unsafePerformSync

      dbRows should have size (3)
    }

    "validate and merge" in {
      val startedAt = LocalDateTime.parse("2016-08-06T00:00:00")
      val forDate = LocalDate.parse("2016-07-21")
      val targetDir = Paths.get("it/small/staging")
      val configText = new String(Files.readAllBytes(Paths.get("it/small/config.json")))
      val xa = initialiseDb()

      val mkPersistRF: Int => PersistRowFailures = id => DatabaseOps.insertRowFailures(id, xa)
      val eachFile = StreamOps.validateFileKeepGood(targetDir, mkPersistRF, FileValidation.tooManyErrors, 16, 2) _

      implicit val S = fs2.Strategy.fromFixedDaemonPool(2, threadName = "worker")

      // need to initialise the database here, call to initialiseJob
      val required = Set[String]("records-a-2016-07-21.txt", "records-b-2016-07-21.txt")
      val (jobId, filenamesAndIds) = DatabaseOps.initialiseJob(xa)(startedAt, required, 
        Set[String]()).unsafePerformSync

      val merged: Vector[(Int, Seq[Seq[String]], Seq[Seq[String]])] = StreamOps.mergeTwo(
          eachFile, 
          ("records-a-2016-07-21.txt", filenamesAndIds("records-a-2016-07-21.txt")), 
          ("records-b-2016-07-21.txt", filenamesAndIds("records-b-2016-07-21.txt"))
        ).runLog.unsafePerformSync

      merged.size should be (3)
      merged(0) should be ((1, 
        Seq(Seq("1", "a", "b"), Seq("1", "a", "b")), 
        Seq(Seq("1", "a", "b"), Seq("1", "a", "b"))
      ))
      merged(1) should be ((2, 
        Seq(Seq("2", "a", "b")), 
        Seq[Seq[String]]()
      ))
      merged(2) should be ((3, 
        Seq(Seq("3", "a", "b"), Seq("3", "a", "b"), Seq("3", "a", "b"), Seq("3", "a", "b")), 
        Seq(Seq("3", "a", "b"))
      ))
    }
    
    "operate on slightly larger data" ignore {
      // What dependencies would I like to be able to sub in?
      //  - the actual checks to run
      //  - the database to store the results in
      
      val startedAt = LocalDateTime.parse("2016-08-06T00:00:00")
      val forDate = LocalDate.parse("2016-07-21")
      val targetDir = Paths.get("it/large/staging")
      val configText = new String(Files.readAllBytes(Paths.get("it/large/config.json")))
      val xa = initialiseDb()

      val mkPersistRF: Int => PersistRowFailures = id => DatabaseOps.insertRowFailures(id, xa)
      val eachFile = StreamOps.validateFile(targetDir, mkPersistRF, FileValidation.tooManyErrors, 4096, 100) _
      val rslt = Main.run(startedAt, configText, forDate, targetDir, xa, eachFile)
      rslt should be (1)

      val dbRows = sql"""
          SELECT r.id, f.file_id, r.row_num, r.message 
          FROM dq_job_row_error r, dq_job_file f 
          WHERE r.job_file_id = f.id
        """.query[(Int, String, Int, String)].list.transact(xa).unsafePerformSync

      println(dbRows.size)
      // dbRows should have size (3)
    }
  }
}
