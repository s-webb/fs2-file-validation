package fs2fv

import fs2._
import scala.collection.mutable

object MergeStreams {

  type Tagged[F] = (Int, F)

  object Tagged {
    def apply[F](tag: Int, f: F): Tagged[F] = (tag, f)
  }

  implicit class TaggedOps[F](val self: Tagged[F]) extends AnyVal {
    def tag: Int = self._1
    def record: F = self._2
  }

  type Buff[A] = Map[Int, Seq[Tagged[A]]]

  object Buff {
    def empty[A]: Buff[A] = Map[Int, Seq[Tagged[A]]]()
  }

  trait GroupOps[A, B, K] {
    def emptyGroup(k: K): A
    def keyOf(a: A): K
    def outputFor(k: K, as: Map[Int, Tagged[A]]): B
    def ordering: Ordering[K]
  }

  /**
   * Joins a stream of records from multiple sources, where each record is tagged to identify it's source, to create a 
   * stream of output records.
   *
   * Records on the input stream are tagged with an integer id representing their input source. Each record has a key.
   * For each key in the input stream, the join will attempt to find one record with that key for each tag value (i.e.
   * one from each originator) to supply to the join operation (GroupOps.outputFor). For any tags where an input
   * record with the correct key is not found, an empty input record (GroupOps.emptyGroup) will be supplied to the 
   * join operation.
   *
   * A is the record type from the input streams
   * B is the output record type
   * K is the type of the key to join input records on
   */
  def joinTagged[F[_], A, B, K](tags: Set[Int])(implicit ops: GroupOps[A, B, K]): Pipe[F, Tagged[A], B] = {
    def go(buff: Buff[A]): Handle[F, Tagged[A]] => Pull[F, B, Unit] = h => {
      h.receiveOption {
        case Some((tagged, h)) =>
          val buff1 = addRecordChunk(tagged, buff)
          createOutputChecked(tags, buff1) match {
            case Some((o, b)) =>
              Pull.output(Chunk.seq(o)) >> go(b)(h)
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

  // def addRecord[A](tr: Tagged[A], buff: Buff[A]): Buff[A] = {
  //   val t = tr.tag
  //   val trs = if (buff contains t) buff(t) :+ tr else Seq(tr)
  //   buff + (t -> trs)
  // }

  def addRecordChunk[A](chunk: Chunk[Tagged[A]], buff: Buff[A]): Buff[A] = {
    // all records in a chunk will have the same tag because they all come from the same inbound stream,
    // so they can just be ++ to any existing entry for that tag
    if (chunk.isEmpty) buff else {
      val t0 = chunk(0)._1
      chunk.indexWhere(_._1 != t0).foreach { d =>
        val t = chunk(d)._1
        throw new RuntimeException(s"All elements in chunk must have same tag, t0: $t0, t: $t")
      }
      // need to benchmark this, not sure what collection gives best perf for ++
      val v = buff.get(t0).map(_ ++ chunk.toVector).getOrElse(chunk.toVector)
      buff + (t0 -> v)
    }
  }

  def createOutputChecked[A, B, K](tags: Set[Int], buff: Buff[A])(implicit ops: GroupOps[A, B, K]): 
      Option[(Seq[B], Buff[A])] = {

    if (buff.keySet == tags) {
      implicit val ord = ops.ordering
      val tagList: Seq[Int] = tags.toSeq.sorted
      val tagBuffers: Seq[Seq[Tagged[A]]] = tagList.map(t => buff(t))
      val tagBufferSizes: Seq[Int] = tagBuffers.map(_.size)
      val indices: Array[Int] = Array.fill(tagBuffers.size)(0)

      def outOfBounds: Boolean = 
        (0 until tagBuffers.size).exists(n => tagBufferSizes(n) <= indices(n))

      val output: mutable.ArrayBuffer[B] = new mutable.ArrayBuffer[B](1024)
      while (!outOfBounds) {
        val values: Seq[Tagged[A]] = (0 until tagBuffers.size).map(n => tagBuffers(n)(indices(n)))
        val lowest: Tagged[A] = values.minBy(ta => ops.keyOf(ta.record))
        val lowestKey: K = ops.keyOf(lowest.record)
        // update the indices, build the output record
        val outputForKey: mutable.Map[Int, Tagged[A]] = mutable.Map()
        (0 until tagBuffers.size).foreach { n =>
          val v: Tagged[A] = values(n)
          if (ops.keyOf(v.record) == lowestKey) {
            // the curr value for tagBuffers(n) matches the lowest key, add this value to the output record
            // and bump the index
            indices(n) += 1
            outputForKey += (tagList(n) -> v)
          } else {
            // the curr value for tagBuffers(n) doesn't match, pad the output record for this tag and don't
            // inc the index
            outputForKey += (tagList(n) -> (tagList(n), ops.emptyGroup(lowestKey)))
          }
        }
        val outputRecord: B = ops.outputFor(lowestKey, outputForKey.toMap)
        output += outputRecord
      }

      if (output.isEmpty) {
        None
      } else {
        // now I just need to build an output buff, where the values are the tagBuffers starting from currIndex
        val outBuff = mutable.Map[Int, Seq[Tagged[A]]] ()
        (0 until tagBuffers.size).foreach { n =>
          if (indices(n) < tagBuffers(n).size) {
            outBuff += (tagList(n) -> tagBuffers(n).drop(indices(n)))
          }
        }

        Some((output, outBuff.toMap))
      }
    } else None
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
    if (lowestRecs.isEmpty) {
      throw new RuntimeException("Can't find lowest key in a buffer that contains no records")
    } else {
      implicit val ord = ops.ordering
      lowestRecs.minBy(ta => ops.keyOf(ta.record))
    }
  }

  def entriesWithKey[A, B, K](k: K, buff: Buff[A])(implicit ops: GroupOps[A, B, K]): Map[Int, Tagged[A]] = {
    val lowestRecs = lowestRecords(buff)
    if (lowestRecs.isEmpty) {
      throw new RuntimeException("Can't find lowest key in a buffer that contains no records")
    } else {
      lowestRecs.filter(tr => ops.keyOf(tr.record) == k).map(tr => (tr.tag, tr)).toMap
    }
  }

  def lowestRecords[A](buff: Buff[A]): Iterable[Tagged[A]] = {
    buff.values.flatMap { vs: Seq[Tagged[A]] => 
      if (!vs.isEmpty) {
        Some(vs(0))
      } else None
    }
  }

  def removeRecordsWithKey[A, B, K](key: K, buff: Buff[A])(implicit ops: GroupOps[A, B, K]): Buff[A] = {
    val out = mutable.Map[Int, Seq[Tagged[A]]]()
    buff.foreach { case (k, vs: Seq[Tagged[A]]) =>
      if (!vs.isEmpty) {
        if (ops.keyOf(vs(0).record) == key) {
          if (vs.size > 1) {
            out += (k -> vs.drop(1))
          }
        } else {
          out += (k -> vs)
        }
      }
    }
    out.toMap
  }
}
