package fs2fv

import scala.language.higherKinds

import fs2._

import org.scalatest.{Matchers, WordSpecLike}

import MergeStreams._
import MergeStreamsExampleSpec._

/**
 * What are the different versions in this file?
 *
 * outerJoin - this joins two streams according to their keys, but it's based on Either so would
 * only works for joining exactly two streams
 *
 * joinTagged - uses an accumulator to store the current lowest key for each stream plus a buffer
 * to store any records received for streams that already have an entry in the accumulator
 *
 * joinTagged2 - get rid of the accumulator, work solely from the buffer (simplify)
 *
 * What could I do now?
 *
 *  - factor out some of this common code
 *  - move the code into a fresh project
 *  - make joinTagged generic
 */

object MergeStreamsExampleSpec {

  // type Acc = Map[Int, Tagged[Record1]]
  // object Acc {
  //   def empty: Acc = Map[Int, Tagged[Record1]]()
  // }

  // type Buff = Seq[Tagged[Record1]]
  // object Buff {
  //   def empty: Buff = Seq[Tagged[Record1]]()
  // }

  type Buff2 = Map[Int, Seq[Tagged[Record1]]]
  object Buff2 {
    def empty: Buff2 = Map[Int, Seq[Tagged[Record1]]]()
  }
}

class MergeStreamsExampleSpec extends WordSpecLike with Matchers {

  // // I don't think this version, where the two streams would be zipped, works.
  // // If we zip the streams arbitrarily then the tuples are very likely to be non-matching
  // def outerJoin[F[_]]: Pipe[F, (Option[(Int, Seq[String])], Option[(Int, Seq[String])]), 
  //     (Int, Seq[String], Seq[String])] = {

  //   def go: Handle[F, (Option[(Int, Seq[String])], Option[(Int, Seq[String])])] => 
  //       Pull[F, (Int, Seq[String], Seq[String]), Unit] = h => {

  //     // pull the first element from the handle
  //     // if both sides are None, we're done (shouldn't happen though)
  //     // if one side is a None, pad the other with an empty Seq and output1
  //     // if both sides are some and equal, output them as a pair
  //     // otherwise, output the lower side, pad the other and recurse with the higher
  //     h.receive1 { (pair, h) =>
  //       case (None, None) => Pull.done
  //       case (Some((k1, vs1)), None) => Pull.output1((k1, vs1, Seq.empty)) >> go(h)
  //       case (None, Some((k2, vs2))) => Pull.output1((k2, Seq.empty, vs2)) >> go(h)
  //       case (Some((k1, vs1)), Some((k2, vs2))) =>
  //         if (k1 == k2) {
  //           Pull.output1((k1, vs1, vs2)) >> go(h)
  //         }
  //     }

  //     ???
  //   }
  //   in => in.pull(go)
  // }

  // this could be a type constructor?


  def outerJoin[F[_]]: Pipe[F, Either[Record1, Record1], Record2] = {

    def go(ls: Seq[Record1], rs: Seq[Record1]): Handle[F, Either[Record1, Record1]] => 
        Pull[F, Record2, Unit] = h => {

      h.receive1Option { 
        case Some((Left(l), h)) =>
          rs.headOption match {
            case Some(r) =>
              val (lk, rk) = (l.key, r.key)
              if (lk == rk) {
                // output l and r, push rs.tail back
                Pull.output1((lk, l.values, r.values)) >> 
                  go(Seq.empty, Seq.empty)(h.push(Chunk.seq(rs.tail.map(Right(_)))))
              } else if (lk < rk) {
                // output l with pad for r, push rs back
                Pull.output1((lk, l.values, Seq.empty)) >> 
                  go(Seq.empty, Seq.empty)(h.push(Chunk.seq(rs.map(Right(_)))))
              } else {
                // lk > rk, output r with pad for l, push l and rs.tail back
                // TODO would it be more efficient to do a sorted insert of l into rs before the push?
                val remainder: Seq[Either[Record1, Record1]] = rs.map(Right(_)) :+ Left(l)
                Pull.output1((rk, Seq.empty, r.values)) >> 
                  go(Seq.empty, Seq.empty)(h.push(Chunk.seq(remainder)))
              }
            case None =>
              // we have no rs, go again
              go(ls :+ l, rs)(h)
          }

        case Some((Right(r), h)) =>
          ls.headOption match {
            case Some(l) =>
              val (lk, rk) = (l.key, r.key)
              if (lk == rk) {
                // output l and r, push ls.tail back
                Pull.output1((lk, l.values, r.values)) >> 
                  go(Seq.empty, Seq.empty)(h.push(Chunk.seq(ls.tail.map(Left(_)))))
              } else if (lk < rk) {
                // output l with pad for r, push ls back
                Pull.output1((lk, l.values, Seq.empty)) >> 
                  go(Seq.empty, Seq.empty)(h.push(Chunk.seq(ls.map(Left(_)))))
              } else {
                // lk > rk, output r with pad for l, push r and ls.tail back
                // TODO would it be more efficient to do a sorted insert of r into ls before the push?
                val remainder: Seq[Either[Record1, Record1]] = ls.map(Left(_)) :+ Right(r)
                Pull.output1((rk, Seq.empty, r.values)) >> 
                  go(Seq.empty, Seq.empty)(h.push(Chunk.seq(remainder)))
              }
            case None =>
              // we have no ls, go again
              go(ls, rs :+ r)(h)
          }

        case None =>
          Pull.output {
            val s = ls.map { l =>
              (l.key, l.values, Seq.empty)
            }
            Chunk.seq(s)
          } >>
          Pull.output {
            val s = rs.map { r =>
              (r.key, Seq.empty, r.values)
            }
            Chunk.seq(s)
          } >>
          Pull.done
      }
    }
    in => in.pull(go(Seq.empty, Seq.empty))
  }

  // def addRecord(tr: Tagged[Record1], acc: Acc, buff: Buff): (Acc, Buff) = {
  //   val t = tr.tag
  //   if (acc contains t) {
  //     // if tag is already present in acc, buffer it
  //     (acc, buff :+ tr)
  //   } else {
  //     // if tag is not present in acc, add it
  //     (acc + (t -> tr), buff)
  //   }
  // }

  // def lowestKey(acc: Acc): Tagged[Record1] = acc.values.minBy(_.record.key)

  // def entriesWithKey(k: Int, acc: Acc): Map[Int, Tagged[Record1]] = acc.filter(_._2.record.key == k)

  // def promote(tag: Int, acc: Acc, buff: Buff): (Acc, Buff) = {
  //   val entry = buff.find(_.tag == tag)
  //   entry.map { e  =>
  //     (acc + (tag -> e), buff.filterNot(_ == e))
  //   }.getOrElse (acc, buff)
  // }

  // def createOutputChecked(tags: Set[Int], acc1: Acc, buff1: Buff): Option[(Record2, Acc, Buff)] =
  //   if (acc1.size == tags.size) Some(createOutput(tags, acc1, buff1)) else None
  
  // def createOutput(tags: Set[Int], acc1: Acc, buff1: Buff): (Record2, Acc, Buff) = {
  //   // if all tags have an entry in acc, do output, how does output work?
  //   // find the lowest key in acc
  //   val lowest = lowestKey(acc1).record.key
  //   // find all entries that have this key
  //   val entriesForOutput = entriesWithKey(lowest, acc1)
  //   // create an output record containing all of those entries, with padding for any non-matching
  //   val missingTags = tags.filterNot(entriesForOutput.keySet.contains)
  //   val paddedEntries: Map[Int, Tagged[Record1]] = 
  //     missingTags.map(t => (t -> Tagged(t, Record1(lowest, Seq.empty)))).toMap
  //   val output: Map[Int, Tagged[Record1]] = entriesForOutput ++ paddedEntries
  //   val acc2 = acc1.filterNot(_._2.record.key == lowest)
  //   // promote the lowest key value for each tag from buff to acc
  //   val (acc3, buff2) = entriesForOutput.foldLeft((acc2, buff1)) { case ((a, b), e) =>
  //     promote(e.tag, a, b)
  //   }
  //   // hard-coded the keys 1 and 2 here, should be a bit more flexible really
  //   val outputRecord: Record2 = (lowest, output(1).record.values, output(2).record.values)
  //   (outputRecord, acc3, buff2)
  // }

  // "createOutput" should {
  //   "create output when all tags have a value" in {
  //     val acc: Map[Int, Tagged[Record1]] = Map(
  //       1 -> Tagged(1, Record1(101, Seq[String]("1.101.a", "1.101.b"))),
  //       2 -> Tagged(2, Record1(101, Seq[String]("2.101.a")))
  //     )
  //     val buff: Seq[Tagged[Record1]] = Seq(
  //       Tagged(1, Record1(102, Seq[String]("1.102.a")))
  //     )
  //     val tags = Set(1, 2)
  //     val (out, acc1, buff1) = createOutput(tags, acc, buff)
  //     out should be (101, Seq("1.101.a", "1.101.b"), Seq("2.101.a"))
  //     acc1 should contain only (1 -> Tagged(1, Record1(102, Seq[String]("1.102.a"))))
  //     buff1 shouldBe empty
  //   }

  //   "create output when not all tags have a value" in {
  //     val acc: Map[Int, Tagged[Record1]] = Map(
  //       1 -> Tagged(1, Record1(101, Seq[String]("1.101.a", "1.101.b")))
  //     )
  //     val buff: Seq[Tagged[Record1]] = Seq(
  //       Tagged(1, Record1(102, Seq[String]("1.102.a")))
  //     )
  //     val tags = Set(1, 2)
  //     val (out, acc1, buff1) = createOutput(tags, acc, buff)
  //     out should be (101, Seq("1.101.a", "1.101.b"), Seq.empty)
  //     acc1 should contain only (1 -> Tagged(1, Record1(102, Seq[String]("1.102.a"))))
  //     buff1 shouldBe empty
  //   }
  // }

  // def joinTagged[F[_]](tags: Set[Int]): Pipe[F, Tagged[Record1], Record2] = {
  //   // this is getting somewhere, but I don't think I actually need separate stores for accumulated and buffered
  //   // values, I could simplify this
  //   def go(acc: Acc, buff: Buff): Handle[F, Tagged[Record1]] => Pull[F, Record2, Unit] = h => {
  //     h.receive1Option {
  //       case Some((tagged@(tag, rec), h)) =>
  //         val (acc1, buff1) = addRecord(tagged, acc, buff)
  //         createOutputChecked(tags, acc1, buff1) match {
  //           case Some((o, a, b)) =>
  //             Pull.output1(o) >> go(a, b)(h)
  //           case None =>
  //             Pull.pure(()) >> go(acc1, buff1)(h)
  //         }
  //       case None if !acc.isEmpty =>
  //         // output whatever we can from acc, then recurse
  //         val (o, a, b) = createOutput(tags, acc, buff)
  //         Pull.output1(o) >> go(a, b)(h)
  //       case None =>
  //         // when acc and h are both empty, we're done
  //         Pull.done
  //     }
  //   }
  //   in => in.pull(go(Acc.empty, Buff.empty))
  // }

  // here's where I have to leave it. Trying to see whether I can just maintain a buffer, rather than
  // separate accumulator and buffer. All depends whether I can implement createOutput using just a buffer.
  // which I'm sure I can!
  def addRecord1(tr: Tagged[Record1], buff: Buff2): Buff2 = {
    val t = tr.tag
    val trs = if (buff contains t) buff(t) :+ tr else Seq(tr)
    buff + (t -> trs)
  }

  /**
   * Create an output element only if buff has at least one element for every tag
   */
  def createOutputChecked1(tags: Set[Int], buff: Buff2): Option[(Record2, Buff2)] = {
    val allTagsBuffered = tags.forall(t => buff.get(t).map(_.size > 0).getOrElse(false))
    if (allTagsBuffered) Some(createOutput1(tags, buff)) else None
  }
  
  def createOutput1(tags: Set[Int], buff: Buff2): (Record2, Buff2) = {
    // if all tags have an entry in acc, do output, how does output work?
    // find the lowest key in acc
    val lowest = lowestKey1(buff).record.key
    // find all entries that have this key
    val entriesForOutput = entriesWithKey1(lowest, buff)
    // create an output record containing all of those entries, with padding for any non-matching
    val missingTags = tags.filterNot(entriesForOutput.keySet.contains)
    val paddedEntries: Map[Int, Tagged[Record1]] = 
      missingTags.map(t => (t -> Tagged(t, Record1(lowest, Seq.empty)))).toMap
    val output: Map[Int, Tagged[Record1]] = entriesForOutput ++ paddedEntries

    // this promotion logic should be easier now, I just need to edit buff to remove
    // the first element from each value if that element's key matches 'lowest'
    val buff2 = removeRecordsWithKey(lowest, buff)

  //   val acc2 = acc1.filterNot(_._2.record.key == lowest)
  //   // promote the lowest key value for each tag from buff to acc
  //   val (acc3, buff2) = entriesForOutput.foldLeft((acc2, buff1)) { case ((a, b), e) =>
  //     promote(e.tag, a, b)
  //   }

    // hard-coded the keys 1 and 2 here, should be a bit more flexible really
    println(s"!!! outputting record with key $lowest")
    val outputRecord: Record2 = (lowest, output(1).record.values, output(2).record.values)
    (outputRecord, buff2)
  }

  def removeRecordsWithKey(key: Int, buff: Buff2): Buff2 = {
    val buff2 = buff.mapValues { vs: Seq[Tagged[Record1]] =>
      val shouldDrop = vs.headOption.map(_.record.key == key).getOrElse(false)
      if (shouldDrop) vs.drop(1) else vs
    }
    buff2.filter(_._2.size > 0)
  }

  "removeRecordsWithKey" should {
    "remove first element of values where the element's key matches the given key" in {
      val buff: Buff2 = Map[Int, Seq[Tagged[Record1]]] (
        1 -> Seq(Tagged(1, (100, Seq("a", "b", "c")) ))
      )
      val buff2 = removeRecordsWithKey(100, buff)
      buff2.size should be (0)
    }
    "not remove first element of values where the element's key does not match the given key" in {
      val buff: Buff2 = Map[Int, Seq[Tagged[Record1]]] (
        1 -> Seq(Tagged(1, (101, Seq("a", "b", "c")) ))
      )
      val buff2 = removeRecordsWithKey(100, buff)
      buff2(1).size should be (1)
    }
    "not fail on empty buff" in {
      val buff: Buff2 = Map[Int, Seq[Tagged[Record1]]] ()
      val buff2 = removeRecordsWithKey(100, buff)
      buff2.size should be (0)
    }
  }

  def lowestRecords(buff: Buff2): Iterable[Tagged[Record1]] = {
    buff.values.flatMap { vs: Seq[Tagged[Record1]] => 
      if (vs.size > 0) {
        Some(vs(0))
      } else None
    }
  }

  def lowestKey1(buff: Buff2): Tagged[Record1] = {
    val lowestRecs = lowestRecords(buff)
    if (lowestRecs.size == 0) {
      throw new RuntimeException("Can't find lowest key in a buffer that contains no records")
    } else {
      lowestRecs.minBy(_.record.key)
    }
  }

  "lowestKey1" should {
    "find lowest key in non-empty buff" in {
      val buff: Buff2 = Map[Int, Seq[Tagged[Record1]]] (
        1 -> Seq(Tagged(1, (100, Seq("a", "b", "c")) ))
      )
      lowestKey1(buff).record.key should be (100)
    }
    "find lowest key when there are multiple keys with different values" in {
      val buff: Buff2 = Map[Int, Seq[Tagged[Record1]]] (
        1 -> Seq(Tagged(1, (100, Seq("a", "b", "c")))),
        2 -> Seq(Tagged(2, (99, Seq("a"))))
      )
      lowestKey1(buff).record.key should be (99)
    }
    "throw an exception if the buffer is empty" in {
      val buff: Buff2 = Map[Int, Seq[Tagged[Record1]]] ()
      assertThrows[RuntimeException] {
        lowestKey1(buff)
      }
    }
    "throw an exception if the buffer only has empty values" in {
      val buff: Buff2 = Map[Int, Seq[Tagged[Record1]]] (
        1 -> Seq()
      )
      assertThrows[RuntimeException] {
        lowestKey1(buff)
      }
    }
  }

  def entriesWithKey1(k: Int, buff: Buff2): Map[Int, Tagged[Record1]] = {
    val lowestRecs = lowestRecords(buff)
    if (lowestRecs.size == 0) {
      throw new RuntimeException("Can't find lowest key in a buffer that contains no records")
    } else {
      lowestRecs.filter(_.record.key == k).map(tr => (tr.tag, tr)).toMap
    }
  }

  def joinTagged2[F[_]](tags: Set[Int]): Pipe[F, Tagged[Record1], Record2] = {
    def go(buff: Buff2): Handle[F, Tagged[Record1]] => Pull[F, Record2, Unit] = h => {

      h.receive1Option {
        case Some((tagged, h)) =>
          val buff1 = addRecord1(tagged, buff)
          println(s"Calling checked createOutput1")
          createOutputChecked1(tags, buff1) match {
            case Some((o, b)) =>
              Pull.output1(o) >> go(b)(h)
            case None =>
              Pull.pure(()) >> go(buff1)(h)
          }

        case None if !buff.isEmpty =>
          // output whatever we can from buff, then recurse
          println(s"Calling unchecked createOutput1")
          val (o, b) = createOutput1(tags, buff)
          Pull.output1(o) >> go(b)(h)

        case None =>
          Pull.done
      }
    }
    in => in.pull(go(Buff2.empty))
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
      val s3 = Stream(((1, "3.1.a"), (1, "3.1.b")))

      // val rslt: Stream[Pure, Stream[Pure, (Int, String)]] = s1.throughPure(GroupKeys.groupKeys1)
      // val rslt: Stream[Pure, Stream[Pure, (Int, String)]] = 
      //   s1.throughPure(GroupKeys.groupKeys2[Pure, (Int, String), Int](_._1, _._1 == _))
      // val rslt: Stream[Pure, (Int, Seq[String])] = s1.throughPure(GroupKeys.groupKeys3)

      // val rslt2 = rslt.toList
      // println(rslt2)

      val s1Groups: Stream[Task, (Int, Seq[String])] = s1.through(GroupKeys.groupKeys3)
      val s2Groups: Stream[Task, (Int, Seq[String])] = s2.through(GroupKeys.groupKeys3)

      // Given that I have two streams, I now need to combine them
      // I'd like to zip them, but only if their grouping keys match. 
      // Can I somehow have handles to both streams open at once?
      // Something in pipe2?
      //
      // What if I interleaved the streams with either? Doesn't quite work though, I
      // need control over when to pull
      //
      // Pseudo-code?
      //
      // - take the first element from both streams
      // - if they match, combine them and move on
      // - if they don't match, take the lower one and combine that with a zero value,
      // then push the higher value back to it's stream to be examined in the next step
      //
      //
      // deterministically merge the streams, then take from the resulting stream until we have a left and a right
      // if they match, output them
      // if the don't, output the lower
      // push anything that hasn't been matched back onto the stream

      implicit val S = fs2.Strategy.fromFixedDaemonPool(2, threadName = "worker")

      val s1AndS2: Stream[Task, Either[Record1, Record1]] = 
        (s1Groups.map(Left(_)) interleaveAll s2Groups.map(Right(_)))

      // {
      //   val rslt: Vector[Either[Record1, Record1]] = s1AndS2.runLog.unsafeRun
      //   println(rslt.mkString("\n"))
      // }


      // {
      //   val joined: Stream[Task, Record2] = s1AndS2.through(outerJoin)
      //   val rsltT: Task[Vector[Record2]] = joined.runLog
      //   val rslt: Vector[Record2] = rsltT.unsafeRun
      //   val rsltStr: String = rslt.map(record2ToString).mkString("\n")
      //   println(s"rslt:\n$rsltStr")
      // }

      {
        val s1Tagged: Stream[Task, Tagged[Record1]] = s1Groups.map((1, _))
        val s2Tagged: Stream[Task, Tagged[Record1]] = s2Groups.map((2, _))
        val s1MergeS2: Stream[Task, Tagged[Record1]] = s1Tagged merge s2Tagged
        val s1JoinS2: Stream[Task, Record2] = s1MergeS2 through joinTagged2(Set(1, 2))
        
        val rsltT: Task[Vector[Record2]] = s1JoinS2.runLog
        val rslt: Vector[Record2] = rsltT.unsafeRun
        val rsltStr: String = rslt.map(record2ToString).mkString("\n")
        println(s"rslt:\n$rsltStr")
      }

      // val expected: Vector[(
      //   Int,
      //   Seq[String],
      //   Seq[String],
      //   Seq[String]
      // )] = Vector(
      //   (1,
      //     Seq("1.1.a", "1.1.b"),
      //     Seq("2.1.a", "2.1.b"),
      //     Seq("3.1.a", "3.1.b")
      //   )
      // )

      // val rslt: Vector[(
      //   Int,
      //   Seq[String],
      //   Seq[String],
      //   Seq[String]
      // )] = ???

      // rslt should be (expected)
    }
  }
}
