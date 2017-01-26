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
          // switched in the optimised version
          createOutputChecked1(tags, buff1) match {
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
    def allTagsBuffered(b: Buff[A]) = tags.forall(t => b.get(t).map(!_.isEmpty).getOrElse(false))
    if (allTagsBuffered(buff)) {
      // this is a bit of a mess :-)
      var b1 = buff

      // would this be any quicker if vv was a mutable collection?
      // nope, if anything it runs more *slowly* on a mutable collection. pfft.
      var vv = Vector[B]()
      // could get some performance improvement here by splitting createOutput into two stages, one
      // that builds up the output buffer, and a second that removes all of the keys that have been included
      // in the output
      while (allTagsBuffered(b1)) {
        val (v, b2) = createOutput(tags, b1)
        vv = (vv :+ v)
        b1 = b2
      }

      Some((vv.toSeq), b1)
    } else None
  }

  def createOutputChecked1[A, B, K](tags: Set[Int], buff: Buff[A])(implicit ops: GroupOps[A, B, K]): 
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

  // pseudo-code for quicker createOutputChecked
  //
  // create an array storing curr index into each tag's buffer (start at 0 :-)
  // get the size of each tag buffer, store that in an array (to save repeatedly calling size)
  // loop
  //   if any of the curr indices is out of bounds, break
  //   get the curr indexed value from each tag buffer
  //   find the lowest of those values
  //   create an output record containing lowest values
  //   pad the output record for any tags where their current value doesn't match the lowest value
  //   for all where curr value == lowest value, increment curr index
  // split all of the tag buffers at curr index 
  // wrap the new buffers into a map and return that and the output buffer
  //
  // A: 1, 2, 3
  // B: 1, 2
  //
  // 0, 0: 1, 1 are output
  // 1, 1: 2, 2 are output
  // 2, 2: curr index out of bounds on B, break
  //
  // A: 1, 4
  // B: 1, 2, 3
  //
  // 0, 0: 1, 1 are output
  // 1, 1: -, 2 are output
  // 1, 2: -, 3 are output
  // 1, 3: curr index out of bounds on B, break
  //
  // A: 1, 4
  // B: 1, 2, 5
  //
  // 0, 0: 1, 1 are output
  // 1, 1: -, 2 are output
  // 1, 2: 4, - are output
  // 2, 2: curr index out of bounds on A, break
  //
  // A: 1, 4
  // B: 1, 2, 5
  // C: 1, 3
  //
  // 0, 0, 0: 1, 1, 1 are output
  // 1, 1, 1: -, -, 3 are output
  // 1, 1, 2: curr index out of bounds on C, break
  //
  // A: 1, 2
  // B: 1, 2, 5
  // C: 1, 3
  //
  // 0, 0, 0: 1, 1, 1 are output
  // 1, 1, 1: -, -, 3 are output

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

  // this is a bit of a hotspot, could do with benchmarking it
  // I'm sure it could be more efficient
  def removeRecordsWithKey[A, B, K](key: K, buff: Buff[A])(implicit ops: GroupOps[A, B, K]): Buff[A] = {
     // val buff2 = buff.mapValues { vs: Seq[Tagged[A]] =>
     //   val shouldDrop = vs.headOption.map(tr => ops.keyOf(tr.record) == key).getOrElse(false)
     //   if (shouldDrop) vs.drop(1) else vs
     // }
     // buff2.filterNot(_._2.isEmpty)
    
    
    // this might be quicker... ?
    val buff2 = mutable.Map[Int, Seq[Tagged[A]]]()
    buff.foreach { case (k, vs: Seq[Tagged[A]]) =>
      if (!vs.isEmpty) {
        if (ops.keyOf(vs(0).record) == key) {
          if (vs.size > 1) {
            buff2 += (k -> vs.drop(1))
          }
        } else {
          buff2 += (k -> vs)
        }
      }
    }
    buff2.toMap
  }
}
