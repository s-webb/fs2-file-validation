package fs2fv

import fs2._

object GroupKeys {

  /**
   * Pipe that groups adjacent elements in a stream by key, where each element in the output stream contains a Seq
   * of multiple elements from the input stream
   *
   * Chunkiness will be preserved to some extent, but the output will, depending on how many consecutive records
   * have the same key, have smaller chunks than the input.
   */
  def groupKeys[F[_], A]: Pipe[F, (Int, A), (Int, Seq[A])] = {

    def go(current: Option[(Int, Seq[A])]): 
        Handle[F, (Int, A)] => Pull[F, (Int, Seq[A]), Unit] = h => {

      h.receiveOption { 
        case Some((chunk, h)) => 
          val (k1, out) = current.getOrElse((chunk(0)._1, Seq[A]()))
          doChunk(chunk, h, k1, out, Vector.empty)
        case None => 
          val l = current.map { case (k1, out) => Pull.output1((k1, out)) } getOrElse Pull.pure(()) 
          l >> Pull.done
      }
    }

    def doChunk(chunk: Chunk[(Int, A)], h: Handle[F, (Int, A)], k1: Int, out: Seq[A], acc: Vector[(Int, Seq[A])]):
        Pull[F, (Int, Seq[A]), Unit] = {

      val differsAt = chunk.indexWhere(_._1 != k1).getOrElse(-1)
      if (differsAt == -1) {
        // whole chunk matches the current key, add this chunk to the current Stream of chunks
        val newOut: Seq[A] = out ++ chunk.toVector.map(_._2)
        if (acc.isEmpty) {
          Pull.pure(()) >> go(Some((k1, newOut)))(h)
        } else {
          // potentially outputs one additional chunk (by splitting the last one in two)
          Pull.output(Chunk.seq(acc)) >> go(Some((k1, newOut)))(h)
        }
      } else {
        // at least part of this chunk does not match the current key, need to group and retain chunkiness
        var startIndex = 0
        var endIndex = differsAt

        // split the chunk into the bit where the keys match and the bit where they don't
        val matching = chunk.take(differsAt)
        // this could certainly be more efficient
        val newOut: Seq[A] = out ++ matching.toVector.map(_._2)
        val nonMatching = chunk.drop(differsAt)
        // nonMatching is guaranteed to be non-empty here, because we know the last element of the chunk doesn't have
        // the same key as the first
        val k2 = nonMatching(0)._1
        doChunk(nonMatching, h, k2, Seq[A](), acc :+ (k1, newOut))
      }
    }

    in => in.pull(go(None))
  }
}

object GroupBy {

  /**
   * Pipe that groups adjacent elements in a stream by key, where each element in the output stream contains a Seq
   * of multiple elements from the input stream
   *
   * Chunkiness will be preserved to some extent, but the output will, depending on how many consecutive records
   * have the same key, have smaller chunks than the input.
   */
  def groupBy[F[_], K, V](f: V => K): Pipe[F, V, (K, Vector[V])] = {

    def go(current: Option[(K, Vector[V])]): 
        Handle[F, V] => Pull[F, (K, Vector[V]), Unit] = h => {

      h.receiveOption { 
        case Some((chunk, h)) => 
          val (k1, out) = current.getOrElse((f(chunk(0)), Vector[V]()))
          doChunk(chunk, h, k1, out, Vector.empty)
        case None => 
          val l = current.map { case (k1, out) => Pull.output1((k1, out)) } getOrElse Pull.pure(()) 
          l >> Pull.done
      }
    }

    def doChunk(chunk: Chunk[V], h: Handle[F, V], k1: K, out: Vector[V], acc: Vector[(K, Vector[V])]):
        Pull[F, (K, Vector[V]), Unit] = {

      val differsAt = chunk.indexWhere(f(_) != k1).getOrElse(-1)
      if (differsAt == -1) {
        // whole chunk matches the current key, add this chunk to the current Stream of chunks
        val newOut: Vector[V] = out ++ chunk.toVector
        if (acc.isEmpty) {
          Pull.pure(()) >> go(Some((k1, newOut)))(h)
        } else {
          // potentially outputs one additional chunk (by splitting the last one in two)
          Pull.output(Chunk.seq(acc)) >> go(Some((k1, newOut)))(h)
        }
      } else {
        // at least part of this chunk does not match the current key, need to group and retain chunkiness
        var startIndex = 0
        var endIndex = differsAt

        // split the chunk into the bit where the keys match and the bit where they don't
        val matching = chunk.take(differsAt)
        // this could certainly be more efficient
        val newOut: Vector[V] = out ++ matching.toVector
        val nonMatching = chunk.drop(differsAt)
        // nonMatching is guaranteed to be non-empty here, because we know the last element of the chunk doesn't have
        // the same key as the first
        val k2 = f(nonMatching(0))
        doChunk(nonMatching, h, k2, Vector[V](), acc :+ (k1, newOut))
      }
    }

    in => in.pull(go(None))
  }
}
