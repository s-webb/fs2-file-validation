package fs2fv

import java.nio.file.{Files, Paths}
import java.time.LocalDate

import doobie.imports._

import org.scalatest.{Matchers, WordSpecLike}

class FileValidationSpec extends WordSpecLike with Matchers with InitialiseDb {

  import FileValidation._

  override def dbName: String = "file-validation"

  "checkFilesPresent" should {

    "record no errors if no required files" in {
      val files = Set[String]()
      val required = Set[RequiredFile]()
      val forDate = LocalDate.of(2016, 7, 21)
      checkFilesPresent(forDate, required, files) should be (Set[RequiredFile]())
    }

    "record name of each missing file" in {
      val files = Set[String]("file1_2016-07-21.txt")
      val file1 = RequiredFile("file1", "'file1_'yyyy-MM-dd'.txt'")
      val file2 = RequiredFile("file2", "'file2_'yyyy-MM-dd'.txt'")
      val required = Set[RequiredFile](file1, file2)
      val forDate = LocalDate.of(2016, 7, 21)
      checkFilesPresent(forDate, required, files) should be (Set[(String, Boolean)](
        ("file1_2016-07-21.txt", true),
        ("file2_2016-07-21.txt", false)
      ))
    }
  }

  "run" should {
    "return 1 for unsuccessful run" in {
      // What dependencies would I like to be able to sub in?
      //  - the actual checks to run
      //  - the database to store the results in
      
      val startedAt = LocalDate.parse("2016-08-06")
      val forDate = LocalDate.parse("2016-07-21")
      val targetDir = Paths.get("it/small/staging")
      val configText = new String(Files.readAllBytes(Paths.get("it/small/config.json")))
      val xa = initialiseDb()

      val eachFile = FileValidation.validateFile(targetDir, xa, 16, 2) _
      val rslt = FileValidation.run(startedAt, configText, forDate, targetDir, xa, eachFile)
      rslt should be (1)

      val dbRows = sql"""
          SELECT r.id, f.file_id, r.row_num, r.message 
          FROM dq_job_row_error r, dq_job_file f 
          WHERE r.job_file_id = f.id
        """.query[(Int, String, Int, String)].list.transact(xa).unsafePerformSync

      dbRows should have size (3)
    }
    
    "operate on slightly larger data" in {
      // What dependencies would I like to be able to sub in?
      //  - the actual checks to run
      //  - the database to store the results in
      
      val startedAt = LocalDate.parse("2016-08-06")
      val forDate = LocalDate.parse("2016-07-21")
      val targetDir = Paths.get("it/large/staging")
      val configText = new String(Files.readAllBytes(Paths.get("it/large/config.json")))
      val xa = initialiseDb()

      val eachFile = FileValidation.validateFile(targetDir, xa, 4096, 100) _
      val rslt = FileValidation.run(startedAt, configText, forDate, targetDir, xa, eachFile)
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
