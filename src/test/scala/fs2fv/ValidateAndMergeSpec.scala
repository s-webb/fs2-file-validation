package fs2fv

import fs2._
import fs2.interop.scalaz._

import java.io.ByteArrayOutputStream

import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.mutable.ArrayBuffer
import scalaz.concurrent.Task

class ValidateAndMergeSpec extends Matchers with WordSpecLike {

  import ValidateAndMerge._

  "validateAndMergeStreams" should {
    "validate and merge streams" in {
      val failures = Seq.fill(2)(new ArrayBuffer[RowFailure]())
      val outputBytes = new ByteArrayOutputStream
      val outputSink: Sink[Task, Byte] = _.mapChunks { c => 
        val arr = c.toArray
        outputBytes.write(arr, 0, arr.length)
        Chunk.empty
      }

      val rawData = Seq(
        """1|a1|b1
          |1|a2|b2
          |1malformed
          |2malformed
          |1|a3|b3
          |2|a1|b1""".stripMargin,
        """1|x1|y1
          |1|x2|y2
          |1|x3|y3
          |3malformed
          |4malformed
          |1|x4|y4
          |3|x1|y1""".stripMargin)

      val unvalidated: Seq[(Stream[Task, Byte], Sink[Task, RowFailure])] = 
        rawData.zipWithIndex.map { case (r, n) => 
          val in = Stream[Task, Byte](r.getBytes:_*)
          val rejects: Sink[Task, RowFailure] = _.map(failures(n) += _)
          (in, rejects)
        }

      val resultS: Stream[Task, Seq[Int]] = validateAndMergeStreams(outputSink)(unvalidated)
      val result: Vector[Seq[Int]] = resultS.runLog.unsafePerformSync
      result should be (Seq(Seq(2, 2)))

      def failureToStr(rf: RowFailure): String = rf match {
        case ((tokens, lineNum), message) => (tokens.toSeq :+ message).mkString("|")
      }

      failures(0).map(failureToStr) should be (Seq(
        "1malformed|Wrong number of tokens", 
        "2malformed|Wrong number of tokens"
      ))
      failures(1).map(failureToStr) should be (Seq(
        "3malformed|Wrong number of tokens", 
        "4malformed|Wrong number of tokens"
      ))

      val expectedOut = """1
        |1|a1|b1
        |1|a2|b2
        |1|a3|b3
        |1|x1|y1
        |1|x2|y2
        |1|x3|y3
        |1|x4|y4
        |2
        |2|a1|b1
        |3
        |3|x1|y1""".stripMargin

      outputBytes.toString should be (expectedOut)
    }
  }
}
