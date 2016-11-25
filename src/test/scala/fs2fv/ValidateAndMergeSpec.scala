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
      val failures = new ArrayBuffer[RowFailure]()
      val failureSink: Sink[Task, RowFailure] = _.map(failures += _)
      val outputBytes = new ByteArrayOutputStream
      val outputSink: Sink[Task, Byte] = _.mapChunks { c => 
        val arr = c.toArray
        outputBytes.write(arr, 0, arr.length)
        Chunk.empty
      }
      val rawData = Seq(
        """1|a1|b1|c1
          |1|a2|b2|c2
          |1malformed
          |2malformed
          |1|a3|b3|c3
          |2|a1|b1|c1
          """.stripMargin,
        """1|x1|y1|z1
          |1|x2|y2|z2
          |1|x3|y3|z3
          |3malformed
          |4malformed
          |1|x4|y4|z4
          |3|x1|y1|z1
          """.stripMargin)
      val bytesIn: Seq[MkByteStream[Task]] = rawData.map(rd =>
        () => Stream[Task, Byte](rd.getBytes:_*)
      )
      val resultS: Stream[Task, Seq[Int]] = validateAndMergeStreams(failureSink, outputSink)(bytesIn)
      val result: Vector[Seq[Int]] = resultS.runLog.unsafePerformSync
      result should be (Seq(Seq(2, 2)))

      // if I also wanted to ensure that all the good data came out correctly, how would I do that?
    }
  }
}
