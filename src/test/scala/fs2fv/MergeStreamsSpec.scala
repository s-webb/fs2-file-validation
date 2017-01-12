package fs2fv

import scala.language.higherKinds

import fs2._

import org.scalatest.{Matchers, WordSpecLike}

import MergeStreams._
import GroupKeys._
import Records._

class MergeStreamsSpec extends WordSpecLike with Matchers {

  "removeRecordsWithKey" should {
    "remove first element of values where the element's key matches the given key" in {
      val buff: Buff[Record1] = Map[Int, Seq[Tagged[Record1]]] (
        1 -> Seq(Tagged(1, (100, Seq("a", "b", "c")) ))
      )
      val buff2 = removeRecordsWithKey(100, buff)
      buff2.size should be (0)
    }
    "not remove first element of values where the element's key does not match the given key" in {
      val buff: Buff[Record1] = Map[Int, Seq[Tagged[Record1]]] (
        1 -> Seq(Tagged(1, (101, Seq("a", "b", "c")) ))
      )
      val buff2 = removeRecordsWithKey(100, buff)
      buff2(1).size should be (1)
    }
    "not fail on empty buff" in {
      val buff: Buff[Record1] = Map[Int, Seq[Tagged[Record1]]] ()
      val buff2 = removeRecordsWithKey(100, buff)
      buff2.size should be (0)
    }
  }

  "lowestKey" should {
    "find lowest key in non-empty buff" in {
      val buff: Buff[Record1] = Map[Int, Seq[Tagged[Record1]]] (
        1 -> Seq(Tagged(1, (100, Seq("a", "b", "c")) ))
      )
      lowestKey(buff).record.key should be (100)
    }
    "find lowest key when there are multiple keys with different values" in {
      val buff: Buff[Record1] = Map[Int, Seq[Tagged[Record1]]] (
        1 -> Seq(Tagged(1, (100, Seq("a", "b", "c")))),
        2 -> Seq(Tagged(2, (99, Seq("a"))))
      )
      lowestKey(buff).record.key should be (99)
    }
    "throw an exception if the buffer is empty" in {
      val buff: Buff[Record1] = Map[Int, Seq[Tagged[Record1]]] ()
      assertThrows[RuntimeException] {
        lowestKey(buff)
      }
    }
    "throw an exception if the buffer only has empty values" in {
      val buff: Buff[Record1] = Map[Int, Seq[Tagged[Record1]]] (
        1 -> Seq()
      )
      assertThrows[RuntimeException] {
        lowestKey(buff)
      }
    }
  }

  "merge" should {
    "group records that have the same identifier" in {
      val s1: Stream[Task, (Int, String)] = Stream(
        (1, "1.1.a"), 
        (1, "1.1.b"), 
        (2, "1.2.a"),
        (2, "1.2.b"),
        (2, "1.2.c")
      )
      val s2: Stream[Task, (Int, String)] = Stream(
        (1, "2.1.a"), 
        (1, "2.1.b")
      )

      implicit val S = fs2.Strategy.fromFixedDaemonPool(2, threadName = "worker")

      val s1tagged = s1.through(groupKeys).map((1, _))
      val s2tagged = s2.through(groupKeys).map((2, _))

      val joined = (s1tagged merge s2tagged).through(joinTagged(Set(1, 2)))

      val rslt: Vector[Record2] = joined.runLog.unsafeRun

      val rsltStr: String = rslt.map(record2ToString).mkString("\n")
      println(s"rslt:\n$rsltStr")

      val expected: Vector[Record2] = Vector(
        (1, Seq("1.1.a", "1.1.b"), Seq("2.1.a", "2.1.b")), 
        (2, Seq("1.2.a", "1.2.b", "1.2.c"), Seq())
      )
      rslt should be (expected)
    }
    
    "group chunked records" in {
      val s1tagged: Stream[Task, (Int, (Int, Seq[String]))] = Stream(
        (1, (1, Seq("1.1.a", "1.1.b"))), 
        (1, (2, Seq("1.2.a", "1.2.b", "1.2.c")))
      )
      val numChunksInS1: Int = s1tagged.chunks.runLog.unsafeRun.size
      val s2tagged: Stream[Task, (Int, (Int, Seq[String]))] = Stream(
        (2, (1, Seq("2.1.a", "2.1.b"))),
        (2, (3, Seq("2.3.a", "2.3.b")))
      )
      val numChunksInS2: Int = s2tagged.chunks.runLog.unsafeRun.size

      implicit val S = fs2.Strategy.fromFixedDaemonPool(2, threadName = "worker")

      val merged = (s1tagged merge s2tagged)
      val joined = merged.through(joinTagged(Set(1, 2)))

      val outChunks: Vector[NonEmptyChunk[Record2]] = joined.chunks.runLog.unsafeRun
      val numOutChunks = outChunks.size

      println(s"s1 chunks: $numChunksInS1, s2 chunks: $numChunksInS2, out chunks: $numOutChunks")
      outChunks.foreach { c =>
        println(s"Size of chunk: ${c.size}")
        c.toList.foreach { e =>
          println(e)
        }
      }

      numOutChunks should be <= (numChunksInS1 + numChunksInS2)

      val rsltStr: String = outChunks.flatMap(_.toVector).map(record2ToString).mkString("\n")
      println(s"rslt:\n$rsltStr")
      val rslt = outChunks.flatMap(_.toVector)

      val expected: Vector[Record2] = Vector(
        (1, Seq("1.1.a", "1.1.b"), Seq("2.1.a", "2.1.b")), 
        (2, Seq("1.2.a", "1.2.b", "1.2.c"), Seq()),
        (3, Seq(), Seq("2.3.a", "2.3.b"))
      )
      rslt should be (expected)
    }

    "process a stream of chunks" in {
      def generateTagged(tag: Int, length: Int): Stream[Task, (Int, (Int, Seq[String]))] = {
        val keys = Stream(1).repeat.take(length).scan(0)((a, v) => a + v)
        keys.map { key =>
          val rs: Seq[String] = (1 until 3).map { n => s"$tag.$key.$n" }
          (tag, (key, rs))
        }
      }
      val tagged1 = generateTagged(1, 4000)
      val tagged2 = generateTagged(2, 3590)

      implicit val S = fs2.Strategy.fromFixedDaemonPool(2, threadName = "worker")

      val merged = (tagged1 merge tagged2)
      val joined = merged.through(joinTagged(Set(1, 2)))

      val before = System.currentTimeMillis
      joined.run.unsafeRun
      val after = System.currentTimeMillis
      val duration = after - before
      println(s"Took $duration millis")
    }
  }

  "addRecordChunk" should {
    "fail if all records in chunk do not have same tag" in {
      val tas: Seq[Tagged[String]] = Seq((1, "1a"), (2, "1b"))
      val msg = "All elements in chunk must have same tag, t0: 1, t: 2"
      the [RuntimeException] thrownBy {
        addRecordChunk(Chunk.seq(tas), Buff.empty[String])
      } should have message msg
    }

    "add records in chunk to empty buffer" in {
      val tas: Seq[Tagged[String]] = Seq((1, "1a"), (1, "1b"))
      val c = Chunk.seq(tas)
      val b1 = Buff.empty[String]
      val b2 = addRecordChunk(c, b1)

      b2.size should be (1)
      b2(1) should be (Seq((1, "1a"), (1, "1b")))
    }

    "add records in chunk to new key in non-empty buffer" in {
      val tas: Seq[Tagged[String]] = Seq((3, "3a"))
      val c = Chunk.seq(tas)
      val b1: Buff[String] = Map(
        1 -> Seq((1, "1a"), (1, "1b"))
      )
      val b2 = addRecordChunk(c, b1)

      b2.size should be (2)
      b2(1) should be (Seq((1, "1a"), (1, "1b")))
      b2(3) should be (Seq((3, "3a")))
    }

    "add records in chunk to existing key in non-empty buffer" in {
      val tas: Seq[Tagged[String]] = Seq((1, "1c"), (1, "1d"))
      val c = Chunk.seq(tas)
      val b1: Buff[String] = Map(
        1 -> Seq((1, "1a"), (1, "1b"))
      )
      val b2 = addRecordChunk(c, b1)

      b2.size should be (1)
      b2(1) should be (Seq((1, "1a"), (1, "1b"), (1, "1c"), (1, "1d")))
    }
  }

  "createOutputChecked" should {
    val ops = new GroupOps[(Int, Seq[String]), String, Int] {
      def emptyGroup(k: Int): (Int, Seq[String]) = (k, Seq[String]())
      def keyOf(a: (Int, Seq[String])): Int = a._1
      def outputFor(k: Int, as: Map[Int, Tagged[(Int, Seq[String])]]): String = {
        val l = as(1)._2._2.mkString(",")
        val r = as(2)._2._2.mkString(",")
        s"$l ++ $r"
      }
      def ordering: Ordering[Int] = implicitly[Ordering[Int]]
    }
    val tags = Set(1, 2)

    "create an element of output when all tags are buffered" in {
      val buff: Buff[(Int, Seq[String])] = Map(
        1 -> Seq((1, (100, Seq("1.100.a", "1.100.b")))),
        2 -> Seq((2, (100, Seq("2.100.a"))))
      )
      val out = createOutputChecked[(Int, Seq[String]), String, Int](tags, buff)(ops)
      val expectedValues = Vector("1.100.a,1.100.b ++ 2.100.a")
      val expectedBuffer = Buff.empty[(Int, Seq[String])]
      out should be (Some((expectedValues, expectedBuffer)))
    }
    
    "create multiple elements of output when multiple keys are buffered for all tags" in {
      val buff: Buff[(Int, Seq[String])] = Map(
        1 -> Seq(
          (1, (100, Seq("1.100.a", "1.100.b"))),
          (1, (200, Seq("1.200.a")))
        ),
        2 -> Seq(
          (2, (100, Seq("2.100.a"))),
          (2, (200, Seq("2.200.a")))
        )
      )
      val out = createOutputChecked[(Int, Seq[String]), String, Int](tags, buff)(ops)
      val expectedValues = Vector("1.100.a,1.100.b ++ 2.100.a", "1.200.a ++ 2.200.a")
      val expectedBuffer = Buff.empty[(Int, Seq[String])]
      out should be (Some((expectedValues, expectedBuffer)))
    }
  }
}
