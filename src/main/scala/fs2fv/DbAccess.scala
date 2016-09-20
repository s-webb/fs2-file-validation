package fs2fv

import java.time.LocalDate

import doobie.imports._

import scalaz._, Scalaz._
import scalaz.concurrent.Task

object DbAccess {

  val xa = DriverManagerTransactor[Task](
    "org.postgresql.Driver", "jdbc:postgresql:fs2fv", "fs2fv_user", "password"
  )

  def insertJob(startedAt: LocalDate): ConnectionIO[Int] =
    sql"insert into dq_job(started_at) values (${startedAt.toString})".update.withUniqueGeneratedKeys("id")

  def insertFile(jobId: Int, fileId: String): ConnectionIO[Int] =
    sql"insert into dq_job_file(job_id, file_id) values ($jobId, $fileId)".update.withUniqueGeneratedKeys("id")

  def insertFileError(fileId: Int, message: String): ConnectionIO[Int] =
    sql"insert into dq_job_file_error(job_file_id, message) values ($fileId, $message)".update.withUniqueGeneratedKeys("id")

  def insertRowError(fileId: Int, rowNum: Int, message: String): ConnectionIO[Int] =
    sql"insert into dq_job_row_error(job_file_id, row_num, message) values ($fileId, $rowNum, $message)".update.withUniqueGeneratedKeys("id")

  def failureToInsert(jobFileId: Int)(f: RowFailure): ConnectionIO[Int] =
    insertRowError(jobFileId, f.rowNum, f.message)

  // def createFileErrors(ps: List[(Int, String, String)]) = {
  //   val sql = "insert into dq_job_file(job_id, file_id, message) values (?, ?, ?)"
  //   Update[(Int, String, String)](sql).updateMany(ps)
  // }

  // def runUpdate(upd: Update0) = xa.trans(upd.run)
}
