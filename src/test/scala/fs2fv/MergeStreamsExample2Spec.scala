package fs2fv

import scala.language.higherKinds

import fs2._

import org.scalatest.{Matchers, WordSpecLike}

import MergeStreams._
import GroupKeys.{groupKeys3 => groupKeys}
import MergeStreamsExample2Spec._

object MergeStreamsExample2Spec {

  type Buff = Map[Int, Seq[Tagged[Record1]]]
  object Buff {
    def empty: Buff = Map[Int, Seq[Tagged[Record1]]]()
  }
}

class MergeStreamsExample2Spec extends WordSpecLike with Matchers {

  def joinTagged[F[_]](tags: Set[Int]): Pipe[F, Tagged[Record1], Record2] = {
    def go(buff: Buff): Handle[F, Tagged[Record1]] => Pull[F, Record2, Unit] = h => {

      h.receive1Option {
        case Some((tagged, h)) =>
          val buff1 = addRecord1(tagged, buff)
          createOutputChecked1(tags, buff1) match {
            case Some((o, b)) =>
              Pull.output1(o) >> go(b)(h)
            case None =>
              Pull.pure(()) >> go(buff1)(h)
          }

        case None if !buff.isEmpty =>
          // output whatever we can from buff, then recurse
          val (o, b) = createOutput1(tags, buff)
          Pull.output1(o) >> go(b)(h)

        case None =>
          Pull.done
      }
    }
    in => in.pull(go(Buff.empty))
  }

  def addRecord1(tr: Tagged[Record1], buff: Buff): Buff = {
    val t = tr.tag
    val trs = if (buff contains t) buff(t) :+ tr else Seq(tr)
    buff + (t -> trs)
  }

  /**
   * Create an output element only if buff has at least one element for every tag
   */
  def createOutputChecked1(tags: Set[Int], buff: Buff): Option[(Record2, Buff)] = {
    val allTagsBuffered = tags.forall(t => buff.get(t).map(_.size > 0).getOrElse(false))
    if (allTagsBuffered) Some(createOutput1(tags, buff)) else None
  }

  def createOutput1(tags: Set[Int], buff: Buff): (Record2, Buff) = {
    val lowest = lowestKey(buff).record.key
    val entriesForOutput = entriesWithKey(lowest, buff)
    // create an output record containing all of those entries, with padding for any non-matching
    val missingTags = tags.filterNot(entriesForOutput.keySet.contains)
    val paddedEntries: Map[Int, Tagged[Record1]] = 
      missingTags.map(t => (t -> Tagged(t, Record1(lowest, Seq.empty)))).toMap
    val output: Map[Int, Tagged[Record1]] = entriesForOutput ++ paddedEntries
    val buff2 = removeRecordsWithKey(lowest, buff)
    // hard-coded the keys 1 and 2 here, should be a bit more flexible really
    val outputRecord: Record2 = (lowest, output(1).record.values, output(2).record.values)
    (outputRecord, buff2)
  }

  def lowestKey(buff: Buff): Tagged[Record1] = {
    val lowestRecs = lowestRecords(buff)
    if (lowestRecs.size == 0) {
      throw new RuntimeException("Can't find lowest key in a buffer that contains no records")
    } else {
      lowestRecs.minBy(_.record.key)
    }
  }

  def entriesWithKey(k: Int, buff: Buff): Map[Int, Tagged[Record1]] = {
    val lowestRecs = lowestRecords(buff)
    if (lowestRecs.size == 0) {
      throw new RuntimeException("Can't find lowest key in a buffer that contains no records")
    } else {
      lowestRecs.filter(_.record.key == k).map(tr => (tr.tag, tr)).toMap
    }
  }

  def lowestRecords(buff: Buff): Iterable[Tagged[Record1]] = {
    buff.values.flatMap { vs: Seq[Tagged[Record1]] => 
      if (vs.size > 0) {
        Some(vs(0))
      } else None
    }
  }

  def removeRecordsWithKey(key: Int, buff: Buff): Buff = {
    val buff2 = buff.mapValues { vs: Seq[Tagged[Record1]] =>
      val shouldDrop = vs.headOption.map(_.record.key == key).getOrElse(false)
      if (shouldDrop) vs.drop(1) else vs
    }
    buff2.filter(_._2.size > 0)
  }

  "removeRecordsWithKey" should {
    "remove first element of values where the element's key matches the given key" in {
      val buff: Buff = Map[Int, Seq[Tagged[Record1]]] (
        1 -> Seq(Tagged(1, (100, Seq("a", "b", "c")) ))
      )
      val buff2 = removeRecordsWithKey(100, buff)
      buff2.size should be (0)
    }
    "not remove first element of values where the element's key does not match the given key" in {
      val buff: Buff = Map[Int, Seq[Tagged[Record1]]] (
        1 -> Seq(Tagged(1, (101, Seq("a", "b", "c")) ))
      )
      val buff2 = removeRecordsWithKey(100, buff)
      buff2(1).size should be (1)
    }
    "not fail on empty buff" in {
      val buff: Buff = Map[Int, Seq[Tagged[Record1]]] ()
      val buff2 = removeRecordsWithKey(100, buff)
      buff2.size should be (0)
    }
  }

  "lowestKey" should {
    "find lowest key in non-empty buff" in {
      val buff: Buff = Map[Int, Seq[Tagged[Record1]]] (
        1 -> Seq(Tagged(1, (100, Seq("a", "b", "c")) ))
      )
      lowestKey(buff).record.key should be (100)
    }
    "find lowest key when there are multiple keys with different values" in {
      val buff: Buff = Map[Int, Seq[Tagged[Record1]]] (
        1 -> Seq(Tagged(1, (100, Seq("a", "b", "c")))),
        2 -> Seq(Tagged(2, (99, Seq("a"))))
      )
      lowestKey(buff).record.key should be (99)
    }
    "throw an exception if the buffer is empty" in {
      val buff: Buff = Map[Int, Seq[Tagged[Record1]]] ()
      assertThrows[RuntimeException] {
        lowestKey(buff)
      }
    }
    "throw an exception if the buffer only has empty values" in {
      val buff: Buff = Map[Int, Seq[Tagged[Record1]]] (
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
