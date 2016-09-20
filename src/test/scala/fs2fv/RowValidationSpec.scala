package fs2fv

import java.time.LocalDate

import doobie.imports._

import fs2.{Pure, Stream}
import fs2.{io, text, Task}
import fs2.util._

import org.scalatest.{Matchers, WordSpecLike}

import scalaz._, Scalaz._
import scalaz.concurrent.Task

class RowValidationSpec extends WordSpecLike with Matchers with InitialiseDb {

  override def dbName: String = "row-validation"

  def validateRows(bytes: Array[Byte]): Seq[RowFailure] = {
    RowValidation.validateBytes(Stream(bytes:_*)).toVector
  }

  "rowValidator" should {
    "return no failures" in {
      val rows = """1a|1b|1c
        |2a|2b|2c
        |3a|3b|3c""".stripMargin
      validateRows(rows.getBytes) shouldBe empty
    }

    "return some failures" in {
      val rows = """1a|1b|1c
        |2a|2b
        |3a|3b|3c""".stripMargin
      validateRows(rows.getBytes) should contain only (RowFailure(1, "found 2 tokens, expected 3"))
    }

    "store errors in the db" in {
      val xa = initialiseDb()

      val startedAt = LocalDate.parse("2016-07-21")
      val jobIdC = DbAccess.insertJob(startedAt)
      val jobId = xa.trans(jobIdC).unsafePerformSync
      jobId should be (1)

      val fileIdC = DbAccess.insertFile(jobId, "file-2016-07-21.txt")
      val fileId = xa.trans(fileIdC).unsafePerformSync
      fileId should be (1)

      val rows = """1a|1b|1c
        |2a|2b
        |3a|3b
        |4a|4b|4c""".stripMargin

      val bytes = rows.getBytes
      val toInsert = DbAccess.failureToInsert(fileId) _
      val inserts: Vector[ConnectionIO[Int]] = 
        RowValidation.validateBytes(Stream(bytes:_*)).map(toInsert).toVector
      val sequenced: ConnectionIO[Vector[Int]] = inserts.sequenceU

      val insertedIds = xa.trans(sequenced).unsafePerformSync
      insertedIds should be (Vector(1, 2))

      val dbRows = sql"select r.id, f.file_id, r.row_num, r.message from dq_job_row_error r, dq_job_file f where r.job_file_id = f.id".
        query[(Int, String, Int, String)].list.transact(xa).unsafePerformSync
    }
  }
}
