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
          println(s"Chunk size at group keys: ${chunk.size}")
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
        // The whole chunk matches the current key, add this chunk to the current Stream of chunks
        val newOut: Seq[A] = out ++ chunk.toVector.map(_._2)
        if (acc.isEmpty) {
          Pull.pure(()) >> go(Some((k1, newOut)))(h)
        } else {
          // can save the final chunk being unnecessarily split in two by looking at the next chunk every time
          // doesn't seem like it's worth doing an extra push for every chunk to save having one additional chunk
          /*
          h.receiveOption {
            case None =>
              // we're done
              Pull.output(Chunk.seq(acc :+ (k1, newOut))) >> Pull.done
            case Some((c, h)) =>
              // we're not!
              Pull.output(Chunk.seq(acc)) >> go(Some((k1, newOut)))(h.push(c))
          }
          */
          // this is the version that potentially outputs one additional chunk (by splitting the last one in two)
          Pull.output(Chunk.seq(acc)) >> go(Some((k1, newOut)))(h)
        }
      } else {
        // at least part of this chunk does not match the current key, I need to group
        // and retain chunkiness
        var startIndex = 0
        var endIndex = differsAt
        while (differsAt != -1) {
          // I'd like to do chunk.indexWhere(startIndex, k != k1), but I don't have that form of indexWhere
          // I could make it

        }
        // would it help to turn the chunk into an array?

        // split the chunk into the bit where the keys match and the bit where they don't
        val matching = chunk.take(differsAt)
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
