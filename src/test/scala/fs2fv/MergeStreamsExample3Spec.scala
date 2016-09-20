package fs2fv

import scala.language.higherKinds

import fs2._

import org.scalatest.{Matchers, WordSpecLike}

import MergeStreams._
import GroupKeys.{groupKeys3 => groupKeys}
import MergeStreamsExample3Spec._

object MergeStreamsExample3Spec {

  type Buff[A] = Map[Int, Seq[Tagged[A]]]
  object Buff {
    def empty[A]: Buff[A] = Map[Int, Seq[Tagged[A]]]()
  }
}

class MergeStreamsExample3Spec extends WordSpecLike with Matchers {

  trait GroupOps[A, B, K] {
    def emptyGroup(k: K): A
    def keyOf(a: A): K
    def outputFor(k: K, as: Map[Int, Tagged[A]]): B
    // pretty sure this is impossible to implement
    def ordering: Ordering[K]
  }

  implicit val record1GroupOps = new GroupOps[Record1, Record2, Int] {
    def emptyGroup(key: Int): Record1 = Record1(key, Seq[String]())
    def keyOf(a: Record1): Int = a.key
    def outputFor(k: Int, as: Map[Int, Tagged[Record1]]): Record2 = 
      (k, as(1).record.values, as(2).record.values)
    // pretty sure this is impossible to implement
    def ordering: Ordering[Int] = implicitly[Ordering[Int]]
  }

  /**
   * A is the tag type (i.e. Int)
   * B is the record type from the input streams (Record1)
   * C is the output record type (Record2)
   */
  def joinTagged[F[_], A, B, K](tags: Set[Int])(implicit ops: GroupOps[A, B, K]): Pipe[F, Tagged[A], B] = {
    def go(buff: Buff[A]): Handle[F, Tagged[A]] => Pull[F, B, Unit] = h => {

      h.receive1Option {
        case Some((tagged, h)) =>
          val buff1 = addRecord(tagged, buff)
          createOutputChecked(tags, buff1) match {
            case Some((o, b)) =>
              Pull.output1(o) >> go(b)(h)
            case None =>
              Pull.pure(()) >> go(buff1)(h)
          }

        case None if !buff.isEmpty =>
          // output whatever we can from buff, then recurse
          val (o, b) = createOutput(tags, buff)
          Pull.output1(o) >> go(b)(h)

        case None =>
          Pull.done
      }
    }
    in => in.pull(go(Buff.empty))
  }

  def addRecord[A](tr: Tagged[A], buff: Buff[A]): Buff[A] = {
    val t = tr.tag
    val trs = if (buff contains t) buff(t) :+ tr else Seq(tr)
    buff + (t -> trs)
  }

  def createOutputChecked[A, B, K](tags: Set[Int], buff: Buff[A])(implicit ops: GroupOps[A, B, K]): Option[(B, Buff[A])] = {
    val allTagsBuffered = tags.forall(t => buff.get(t).map(_.size > 0).getOrElse(false))
    if (allTagsBuffered) Some(createOutput(tags, buff)) else None
  }

  def createOutput[A, B, K](tags: Set[Int], buff: Buff[A])(implicit ops: GroupOps[A, B, K]): (B, Buff[A]) = {
    val lowest: K = ops.keyOf(lowestKey(buff).record)
    val entriesForOutput = entriesWithKey(lowest, buff)
    // create an output record containing all of those entries, with padding for any non-matching
    val missingTags = tags.filterNot(entriesForOutput.keySet.contains)
    val paddedEntries: Map[Int, Tagged[A]] = missingTags.map(t => (t -> Tagged[A](t, ops.emptyGroup(lowest)))).toMap
    val output: Map[Int, Tagged[A]] = entriesForOutput ++ paddedEntries
    val buff2 = removeRecordsWithKey(lowest, buff)
    val outputRecord: B = ops.outputFor(lowest, output)
    (outputRecord, buff2)
  }

  def lowestKey[A, B, K](buff: Buff[A])(implicit ops: GroupOps[A, B, K]): Tagged[A] = {
    val lowestRecs = lowestRecords(buff)
    if (lowestRecs.size == 0) {
      throw new RuntimeException("Can't find lowest key in a buffer that contains no records")
    } else {
      implicit val ord = ops.ordering
      lowestRecs.minBy(ta => ops.keyOf(ta.record))
    }
  }

  def entriesWithKey[A, B, K](k: K, buff: Buff[A])(implicit ops: GroupOps[A, B, K]): Map[Int, Tagged[A]] = {
    val lowestRecs = lowestRecords(buff)
    if (lowestRecs.size == 0) {
      throw new RuntimeException("Can't find lowest key in a buffer that contains no records")
    } else {
      lowestRecs.filter(tr => ops.keyOf(tr.record) == k).map(tr => (tr.tag, tr)).toMap
    }
  }

  def lowestRecords[A](buff: Buff[A]): Iterable[Tagged[A]] = {
    buff.values.flatMap { vs: Seq[Tagged[A]] => 
      if (vs.size > 0) {
        Some(vs(0))
      } else None
    }
  }

  def removeRecordsWithKey[A, B, K](key: K, buff: Buff[A])(implicit ops: GroupOps[A, B, K]): Buff[A] = {
    val buff2 = buff.mapValues { vs: Seq[Tagged[A]] =>
      val shouldDrop = vs.headOption.map(tr => ops.keyOf(tr.record) == key).getOrElse(false)
      if (shouldDrop) vs.drop(1) else vs
    }
    buff2.filter(_._2.size > 0)
  }

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
