package fs2fv

import java.nio.file.{Files, Paths}
import java.time.{LocalDate, LocalDateTime}

import doobie.imports._

import org.scalatest.{Matchers, WordSpecLike}

import fs2._

class FileValidationSpec extends WordSpecLike with Matchers with InitialiseDb {

  import FileValidation._

  override def dbName: String = "file-validation"

  "checkFilesPresent" should {

    "record no errors if no required files" in {
      val files = Set[String]()
      val required = Set[RequiredFile]()
      val forDate = LocalDate.of(2016, 7, 21)

      val requiredAt = requiredForDate(required, forDate)
      val missing = requiredAt -- files

      missing should be (Set[RequiredFile]())
    }

    "record name of each missing file" in {
      val files = Set[String]("file1_2016-07-21.txt")
      val file1 = RequiredFile("file1", "'file1_'yyyy-MM-dd'.txt'")
      val file2 = RequiredFile("file2", "'file2_'yyyy-MM-dd'.txt'")
      val required = Set[RequiredFile](file1, file2)
      val forDate = LocalDate.of(2016, 7, 21)

      val requiredAt = requiredForDate(required, forDate)
      val missing = requiredAt -- files

      requiredAt should be (Set[String](
        ("file1_2016-07-21.txt"),
        ("file2_2016-07-21.txt")
      ))

      missing should be (Set[String](
        ("file2_2016-07-21.txt")
      ))
    }
  }

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
    
    "operate on slightly larger data" ignore {
      // What dependencies would I like to be able to sub in?
      //  - the actual checks to run
      //  - the database to store the results in
      
      val startedAt = LocalDateTime.parse("2016-08-06T00:00:00")
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

  "countFailuresPipe" should {
    "map from passes and failures to passes and count" in {

      val s: Stream[Nothing, Either[RowFailure, Seq[String]]] = Stream(
        Right(Seq[String]("a1", "b1", "c1")),
        Left(RowFailure(1, "msg")),
        Left(RowFailure(2, "msg")),
        Right(Seq[String]("a2", "b2", "c2")),
        Left(RowFailure(3, "msg")),
        Right(Seq[String]("a3", "b3", "c3"))
      )

      val o: Seq[((Int, Int), Seq[String])] = s.throughPure(countAndPasses).toVector
      o should equal (Vector(
        ((0, 1), Seq[String]("a1", "b1", "c1")),
        ((2, 2), Seq[String]("a2", "b2", "c2")),
        ((3, 3), Seq[String]("a3", "b3", "c3"))
      ))
    }
  }
}
