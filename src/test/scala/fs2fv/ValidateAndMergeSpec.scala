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

      val unvalidated: Seq[(Stream[Task, Byte], Sink[Task, RowFailure], RowValidator[Task])] = 
        rawData.zipWithIndex.map { case (r, n) => 
          val in = Stream[Task, Byte](r.getBytes:_*)
          val rejects: Sink[Task, RowFailure] = _.map(failures(n) += _)
          (in, rejects, rowValidator[Task])
        }

      val resultS: Stream[Task, Seq[Int]] = validateAndMergeStreams(outputSink)(unvalidated)
      val result: Vector[Seq[Int]] = resultS.runLog.unsafePerformSync
      result should be (Seq(Seq(2, 2)))

      def failureToStr(rf: RowFailure): Seq[String] = rf match {
        case ((tokens, lineNum), messages) => 
          messages.map { message =>
            (tokens.toSeq :+ message).mkString("|")
          }
      }

      failures(0).map(failureToStr).toSeq should be (Seq(
        Seq("1malformed|Wrong number of tokens"), 
        Seq("2malformed|Wrong number of tokens")
      ))
      failures(1).map(failureToStr).toSeq should be (Seq(
        Seq("3malformed|Wrong number of tokens"), 
        Seq("4malformed|Wrong number of tokens")
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
        |3|x1|y1
        |""".stripMargin

      outputBytes.toString should be (expectedOut)
    }
  }

  "outputPipe" should {
    "preserve chunkiness" in {
      def mkRecord(key: Int): OutputRecord = {
        (key, Seq( // 1 is the key
          //  fails, passes    token 0,    token 1,     line num
          Seq(((0, 0), (Array(s"1.$key.a", s"1.$key.b"), 1))), // group of values from first tagged stream
          Seq(((0, 0), (Array(s"2.$key.a", s"2.$key.b"), 1))) // group of values from second tagged stream
        ))
      }
      val os: Seq[Stream[Nothing, OutputRecord]] = 
          (0 until 3).toSeq.map(_ * 5).map(n => Stream.emits((n until (n + 5)).map(mkRecord)))
      val s1: Stream[Nothing, OutputRecord] = os.foldLeft(Stream.empty[Nothing, OutputRecord])(_ ++ _)

      s1.chunks.toVector.size should equal(3)

      val s2: Stream[Nothing, Byte] = s1.throughPure(outputPipe)
      s2.chunks.toVector.map(bs => new String(bs.toArray)).foreach(println)
      s2.chunks.toVector.size should equal(3)
    }
  }

  "failuresToBytes" should {
    "preserve chunkiness" in {
      val key = 1
      val fs1: Seq[RowFailure] = Seq(
        ((Array(s"1a", s"1b"), 1), Seq[String]("fail")),
        ((Array(s"2a", s"2b"), 2), Seq[String]("fail"))
      )
      val fs2: Seq[RowFailure] = Seq(
        ((Array(s"3a", s"3b"), 3), Seq[String]("fail")),
        ((Array(s"4a", s"4b"), 4), Seq[String]("fail"))
      )
      val s: Stream[Nothing, RowFailure] = Stream.emits(fs1) ++ Stream.emits(fs2)
      s.chunks.toVector should have size 2
      val s2 = s.throughPure(failuresToBytes)
      val rslt = new String(s2.toVector.toArray)
      val expected = """1a|1b|fail
          |2a|2b|fail
          |3a|3b|fail
          |4a|4b|fail
          |""".stripMargin
      rslt should be (expected)
      s2.chunks.toVector should have size 2
    }
  }

  def rowValidator[F[_]]: Pipe[F, TokenizedLine, Either[RowFailure, TokenizedLine]] = 
    _.map { case ln@(tokens, linenum) =>
      if (tokens.size == 3) {
        Right(ln)
      } else {
        Left((ln, Seq("Wrong number of tokens")))
      }
    }
}
