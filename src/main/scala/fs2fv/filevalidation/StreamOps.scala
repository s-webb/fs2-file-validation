package fs2fv.filevalidation

import fs2fv.{RowFailure, RowValidation}

import fs2.{concurrent, io, Pipe, Sink, Stream}
// need to use scalaz Task rather than fs2 Task for interop with doobie,
// so using the scalaz Task Effect from fs2/scalaz interop module
import fs2.interop.scalaz._

import java.nio.file.Path

import scalaz._, Scalaz._
import scalaz.concurrent.Task

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

  def validateFileKeepGood(targetDir: Path, mkPersistRF: Int => PersistRowFailures, errorThreshold: ErrorThreshold,
      fileChunkSizeBytes: Int = 4096, persistBatchSize: Int = 100)
      (filename: String, fileId: Int): Stream[Task, ((Int, Int), Seq[String])] = {

    val bytes: Stream[Task, Byte] = io.file.readAll[Task](targetDir.resolve(filename), fileChunkSizeBytes)
    bytes.through(validateFileBytes(filename, mkPersistRF(fileId), persistBatchSize, errorThreshold))
  }

  import fs2fv.MergeStreams._
  import fs2fv.GroupKeys._

  type R1 = (Int, Seq[Seq[String]])
  type R2 = (Int, Seq[Seq[String]], Seq[Seq[String]])

  def mergeTwo(validate: (String, Int) => Stream[Task, ((Int, Int), Seq[String])], 
      first: (String, Int), second: (String, Int)): Stream[Task, R2] = {

    val firstTagged = validate(first._1, first._2).through(keyed).through(groupKeys).map((1, _))
    val secondTagged = validate(second._1, second._2).through(keyed).through(groupKeys).map((2, _))

    implicit val groupOps = new GroupOps[R1, R2, Int] {
      def emptyGroup(key: Int) = (key, Seq[Seq[String]]())
      def keyOf(a: R1): Int = a._1
      def outputFor(k: Int, as: Map[Int, Tagged[R1]]): R2 = (k, as(1).record._2, as(2).record._2)
      def ordering: scala.math.Ordering[Int] = implicitly[scala.math.Ordering[Int]]
    }

    val joined = (firstTagged merge secondTagged).through(joinTagged(Set(1, 2)))
    joined
  }

  // take a stream of (counts, tokens) and map to a stream of (key, tokens) by dropping the counts
  // and turning the first token into an int
  def keyed[F[_]]: Pipe[F, ((Int, Int), Seq[String]), (Int, Seq[String])] = in => {
    in.map {
      case (counts, tokens) => (tokens(0).toInt, tokens)
    }
  }

  private def validateFileBytes(filename: String, 
      persistRF: PersistRowFailures, persistBatchSize: Int,
      tooManyErrors: (Int, Int) => Boolean): Pipe[Task, Byte, ((Int, Int), Seq[String])] = bytes => {

    validateBytes(bytes, Some(filename)).
    observe(sinkRowFailures(persistRF, persistBatchSize)).
    through(countsAndPasses).
    takeWhile { case ((f, p), _) => !tooManyErrors(f, p) }
  }

  def countsAndPasses[F[_]]: Pipe[F, Either[RowFailure, Seq[String]], ((Int, Int), Seq[String])] = in => {
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
