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
  }
}
