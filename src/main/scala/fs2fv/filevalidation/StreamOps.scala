package fs2fv.filevalidation

import fs2fv.{RowFailure, RowValidation}
import fs2fv.FileValidation.{ErrorThreshold, PersistRowFailures}

import fs2.{concurrent, io, Pipe, Sink, Stream}
// need to use scalaz Task rather than fs2 Task for interop with doobie,
// so using the scalaz Task Effect from fs2/scalaz interop module
import fs2.interop.scalaz._

import java.nio.file.Path

import scalaz._, Scalaz._
import scalaz.concurrent.Task

sealed trait StreamOpsDsl[A]

case class ValidateFile(filename: String, fileId: Int) extends StreamOpsDsl[(Int, Int)]

object StreamOpsFree {

  class Ops[S[_]](implicit s0: StreamOpsDsl :<: S) {

    def validateFile(filename: String, fileId: Int): Free[S, (Int, Int)] = {
      Free.liftF(s0.inj(ValidateFile(filename, fileId)))
    }
  }

  object Ops {
    implicit def apply[S[_]](implicit s0: StreamOpsDsl :<: S): Ops[S] =
      new Ops[S]
  }
}

class StreamOpsInterpreter(targetDir: Path, mkPersistRF: Int => PersistRowFailures, 
    errorThreshold: ErrorThreshold, fileChunkSizeBytes: Int = 4096, persistBatchSize: Int = 100)
    extends (StreamOpsDsl ~> Task) {

  val validateFileOp = StreamOps.validateFile(
    targetDir, mkPersistRF, errorThreshold, fileChunkSizeBytes, persistBatchSize) _

  def apply[A](dsl: StreamOpsDsl[A]): Task[A] = dsl match {
    case ValidateFile(filename, fileId) =>
      validateFileOp(filename, fileId)
  }
}

/**
 * This is a namespace for all stream operations related to file validation
 */
object StreamOps {

  import RowValidation.validateBytes

  def validateFile(targetDir: Path, mkPersistRF: Int => PersistRowFailures, errorThreshold: ErrorThreshold,
      fileChunkSizeBytes: Int = 4096, persistBatchSize: Int = 100)
      (filename: String, fileId: Int): Task[(Int, Int)] = {

    val bytes: Stream[Task, Byte] = io.file.readAll[Task](targetDir.resolve(filename), fileChunkSizeBytes)
    val fileValidator: Stream[Task, ((Int, Int), Seq[String])] = 
      bytes.through(validateFileBytes(filename, mkPersistRF(fileId), persistBatchSize, errorThreshold))

    // the runLast here discards the stream of valid data...
    fileValidator.runLast.map(_.map(_._1).getOrElse((0, 0)))
  }

  private def validateFileBytes(filename: String, 
      persistRF: PersistRowFailures, persistBatchSize: Int,
      tooManyErrors: (Int, Int) => Boolean): Pipe[Task, Byte, ((Int, Int), Seq[String])] = bytes => {

    validateBytes(bytes, Some(filename)).
    observe(sinkRowFailures(persistRF, persistBatchSize)).
    through(countsAndPasses).
    takeWhile { case ((f, p), _) => !tooManyErrors(f, p) }
  }

  private def countsAndPasses[F[_]]: Pipe[F, Either[RowFailure, Seq[String]], ((Int, Int), Seq[String])] = in => {
    val z: ((Int, Int), Seq[String]) = ((0, 0), Seq[String]())
    val counted = in.scan (z) { 
      case (((f, p), _), Left(failure)) => ((f + 1, p), Seq[String]())
      case (((f, p), _), Right(pass)) => ((f, p + 1), pass)
    }
    counted.filter(_._2.size > 0)
  }

  private def sinkRowFailures(persist: PersistRowFailures, 
      persistBatchSize: Int = 1000): Sink[Task, Either[RowFailure, Seq[String]]] = {

    in: Stream[Task, Either[RowFailure, Seq[String]]] => {
      val failures: Stream[Task, RowFailure] = in.collect { case Left(f) => f }
      val failuresChunked: Stream[Task, Vector[RowFailure]] = failures.vectorChunkN(persistBatchSize)

      val errIdsNested: Stream[Task, Stream[Task, Vector[Int]]] = failuresChunked.map(c => Stream.eval(persist(c)))
      concurrent.join(3)(errIdsNested).map(_ => ())
    }
  }
}
