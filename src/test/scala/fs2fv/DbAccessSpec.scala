package fs2fv

import java.time.LocalDateTime

import org.scalatest.{Matchers, WordSpecLike}

import doobie.imports._

import scalaz._, Scalaz._
import scalaz.concurrent.Task

class DbAccessSpec extends WordSpecLike with Matchers with InitialiseDb {

  override def dbName = "db-access"

  "DbAccess" should {

    "insert records" in {
      val xa = initialiseDb()
      val startedAt = LocalDateTime.parse("2016-07-21T00:00:00")
      val jobIdC = DbAccess.insertJob(startedAt)
      val jobId = xa.trans(jobIdC).unsafePerformSync
      jobId should be (1)

      val fileIdC = DbAccess.insertFile(jobId, "file-2016-07-21.txt")
      val fileId = xa.trans(fileIdC).unsafePerformSync
      fileId should be (1)

      val fileErrorC = DbAccess.insertFileError(fileId, "missing")
      val fileErrorId = xa.trans(fileErrorC).unsafePerformSync
      fileErrorId should be (1)

      val rowErrorC = DbAccess.insertRowError(fileId, 100, "bad row")
      val rowErrorId = xa.trans(rowErrorC).unsafePerformSync
      rowErrorId should be (1)
    }
  }
}
