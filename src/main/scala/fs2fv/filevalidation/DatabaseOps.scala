package fs2fv.filevalidation

import com.typesafe.scalalogging.StrictLogging

import doobie.imports._

import fs2fv.{DbAccess, RowFailure}

import java.time.LocalDateTime

import scalaz._, Scalaz._
import scalaz.concurrent.Task

sealed trait DatabaseOpsDsl[A]

case class InitialiseJob(startedAt: LocalDateTime, requiredAt: Set[String], 
  missing: Set[String]) extends DatabaseOpsDsl[(Int, Map[String, Int])]

object DatabaseOpsFree {

  class Ops[S[_]](implicit s0: DatabaseOpsDsl :<: S) {

    def initialiseJob(startedAt: LocalDateTime, requiredAt: Set[String], 
        missing: Set[String]): Free[S, (Int, Map[String, Int])] = {
      Free.liftF(s0.inj(InitialiseJob(startedAt, requiredAt, missing)))
    }
  }

  object Ops {
    implicit def apply[S[_]](implicit s0: DatabaseOpsDsl :<: S): Ops[S] =
      new Ops[S]
  }
}

class DatabaseOpsInterpreter(xa: Transactor[Task]) extends (DatabaseOpsDsl ~> Task) {

  def apply[A](dsl: DatabaseOpsDsl[A]): Task[A] = dsl match {
    case InitialiseJob(startedAt, requiredAt, missing) =>
      DatabaseOps.initialiseJob(xa)(startedAt, requiredAt, missing)
  }
}

/**
 * This is a namespace for all database operations related to file validation
 */
object DatabaseOps extends StrictLogging {

  def initialiseJob(xa: Transactor[Task])(startedAt: LocalDateTime, requiredAt: Set[String], 
      missing: Set[String]): Task[(Int, Map[String, Int])] = {

    val jobDetails: ConnectionIO[(Int, Map[String, Int])] = 
      for {
        jobId <- DbAccess.insertJob(startedAt)
        filenamesAndIds <- recordFiles(jobId, requiredAt)
        _ <- recordMissing(missing, filenamesAndIds)
      } yield {
        (jobId, filenamesAndIds)
      }

    xa.trans(jobDetails)
  }

  private def recordFiles(jobId: Int, files: Set[String]): ConnectionIO[Map[String, Int]] = {
    val filenamesAndIdsC: List[ConnectionIO[(String, Int)]] = files.toList.map { filename =>
      DbAccess.insertFile(jobId, filename).map(id => (filename, id))
    }
    filenamesAndIdsC.sequenceU.map(_.toMap)
  }

  private def recordMissing(missing: Set[String], 
      filenamesAndIds: Map[String, Int]): ConnectionIO[Set[(String, Int)]] = {

    val missingErrorsC: List[ConnectionIO[(String, Int)]] = missing.toList.map { filename =>
      DbAccess.insertFileError(filenamesAndIds(filename), "missing").map(id => (filename, id))
    }
    missingErrorsC.sequenceU.map(_.toSet)
  }

  def insertRowFailures(fileId: Int, xa: Transactor[Task]): PersistRowFailures = is => {
    val inserts: Vector[ConnectionIO[Int]] = is.map(DbAccess.failureToInsert(fileId)(_))
    val sequenced: ConnectionIO[Vector[Int]] = inserts.sequenceU
    for {
      _ <- Task.delay(logger.info(s"Inserting row error(s), size ${is.size}"))
      ids <- xa.trans(sequenced)
    } yield ids
  }
}
