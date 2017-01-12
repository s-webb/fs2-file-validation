package fs2fv

import fs2._

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
    def allTagsBuffered(b: Buff[A]) = tags.forall(t => b.get(t).map(!_.isEmpty).getOrElse(false))
    if (allTagsBuffered(buff)) {
      // this is a bit of a mess :-)
      var b1 = buff
      var vv = Vector[B]()
      while (allTagsBuffered(b1)) {
        val (v, b2) = createOutput(tags, b1)
        vv = (vv :+ v)
        b1 = b2
      }
      Some((vv.toSeq), b1)
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

  // this is a bit of a hotspot, could do with benchmarking it
  // I'm sure it could be more efficient
  def removeRecordsWithKey[A, B, K](key: K, buff: Buff[A])(implicit ops: GroupOps[A, B, K]): Buff[A] = {
    val buff2 = buff.mapValues { vs: Seq[Tagged[A]] =>
      val shouldDrop = vs.headOption.map(tr => ops.keyOf(tr.record) == key).getOrElse(false)
      if (shouldDrop) vs.drop(1) else vs
    }
    buff2.filterNot(_._2.isEmpty)
  }
}
