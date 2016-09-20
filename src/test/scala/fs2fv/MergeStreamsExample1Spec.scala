package fs2fv

import scala.language.higherKinds

import fs2._

import org.scalatest.{Matchers, WordSpecLike}

import MergeStreams._
import GroupKeys.{groupKeys3 => groupKeys}
import MergeStreamsExample1Spec._

object MergeStreamsExample1Spec {

  type Acc = Map[Int, Tagged[Record1]]
  object Acc {
    def empty: Acc = Map[Int, Tagged[Record1]]()
  }

  type Buff = Seq[Tagged[Record1]]
  object Buff {
    def empty: Buff = Seq[Tagged[Record1]]()
  }
}

class MergeStreamsExample1Spec extends WordSpecLike with Matchers {

  def joinTagged[F[_]](tags: Set[Int]): Pipe[F, Tagged[Record1], Record2] = {
    def go(acc: Acc, buff: Buff): Handle[F, Tagged[Record1]] => Pull[F, Record2, Unit] = h => {
      h.receive1Option {
        case Some((tagged@(tag, rec), h)) =>
          val (acc1, buff1) = addRecord(tagged, acc, buff)
          createOutputChecked(tags, acc1, buff1) match {
            case Some((o, a, b)) =>
              Pull.output1(o) >> go(a, b)(h)
            case None =>
              Pull.pure(()) >> go(acc1, buff1)(h)
          }
        case None if !acc.isEmpty =>
          // output whatever we can from acc, then recurse
          val (o, a, b) = createOutput(tags, acc, buff)
          Pull.output1(o) >> go(a, b)(h)
        case None =>
          // when acc and h are both empty, we're done
          Pull.done
      }
    }
    in => in.pull(go(Acc.empty, Buff.empty))
  }

  def addRecord(tr: Tagged[Record1], acc: Acc, buff: Buff): (Acc, Buff) = {
    val t = tr.tag
    if (acc contains t) {
      // if tag is already present in acc, buffer it
      (acc, buff :+ tr)
    } else {
      // if tag is not present in acc, add it
      (acc + (t -> tr), buff)
    }
  }

  def createOutputChecked(tags: Set[Int], acc1: Acc, buff1: Buff): Option[(Record2, Acc, Buff)] =
    if (acc1.size == tags.size) Some(createOutput(tags, acc1, buff1)) else None
  
  def createOutput(tags: Set[Int], acc1: Acc, buff1: Buff): (Record2, Acc, Buff) = {
    // if all tags have an entry in acc, do output, how does output work?
    // find the lowest key in acc
    val lowest = lowestKey(acc1).record.key
    // find all entries that have this key
    val entriesForOutput = entriesWithKey(lowest, acc1)
    // create an output record containing all of those entries, with padding for any non-matching
    val missingTags = tags.filterNot(entriesForOutput.keySet.contains)
    val paddedEntries: Map[Int, Tagged[Record1]] = 
      missingTags.map(t => (t -> Tagged(t, Record1(lowest, Seq.empty)))).toMap
    val output: Map[Int, Tagged[Record1]] = entriesForOutput ++ paddedEntries
    val acc2 = acc1.filterNot(_._2.record.key == lowest)
    // promote the lowest key value for each tag from buff to acc
    val (acc3, buff2) = entriesForOutput.foldLeft((acc2, buff1)) { case ((a, b), e) =>
      promote(e.tag, a, b)
    }
    // hard-coded the keys 1 and 2 here, should be a bit more flexible really
    val outputRecord: Record2 = (lowest, output(1).record.values, output(2).record.values)
    (outputRecord, acc3, buff2)
  }

  def lowestKey(acc: Acc): Tagged[Record1] = acc.values.minBy(_.record.key)

  def entriesWithKey(k: Int, acc: Acc): Map[Int, Tagged[Record1]] = acc.filter(_._2.record.key == k)

  def promote(tag: Int, acc: Acc, buff: Buff): (Acc, Buff) = {
    val entry = buff.find(_.tag == tag)
    entry.map { e  =>
      (acc + (tag -> e), buff.filterNot(_ == e))
    }.getOrElse (acc, buff)
  }

  "createOutput" should {
    "create output when all tags have a value" in {
      val acc: Map[Int, Tagged[Record1]] = Map(
        1 -> Tagged(1, Record1(101, Seq[String]("1.101.a", "1.101.b"))),
        2 -> Tagged(2, Record1(101, Seq[String]("2.101.a")))
      )
      val buff: Seq[Tagged[Record1]] = Seq(
        Tagged(1, Record1(102, Seq[String]("1.102.a")))
      )
      val tags = Set(1, 2)
      val (out, acc1, buff1) = createOutput(tags, acc, buff)
      out should be (101, Seq("1.101.a", "1.101.b"), Seq("2.101.a"))
      acc1 should contain only (1 -> Tagged(1, Record1(102, Seq[String]("1.102.a"))))
      buff1 shouldBe empty
    }

    "create output when not all tags have a value" in {
      val acc: Map[Int, Tagged[Record1]] = Map(
        1 -> Tagged(1, Record1(101, Seq[String]("1.101.a", "1.101.b")))
      )
      val buff: Seq[Tagged[Record1]] = Seq(
        Tagged(1, Record1(102, Seq[String]("1.102.a")))
      )
      val tags = Set(1, 2)
      val (out, acc1, buff1) = createOutput(tags, acc, buff)
      out should be (101, Seq("1.101.a", "1.101.b"), Seq.empty)
      acc1 should contain only (1 -> Tagged(1, Record1(102, Seq[String]("1.102.a"))))
      buff1 shouldBe empty
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
