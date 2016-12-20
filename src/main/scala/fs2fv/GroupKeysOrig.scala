package fs2fv

import fs2._

/**
 * A copy of the version of GroupKeys that fails to preserve any chunkiness
 * (and pushes a lot of data back to the input)
 */
object GroupKeysOrig {

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
          doChunk(chunk, h, k1, out)
        case None => 
          val l = current.map { case (k1, out) => Pull.output1((k1, out)) } getOrElse Pull.pure(()) 
          l >> Pull.done
      }
    }

    def doChunk(chunk: Chunk[(Int, A)], h: Handle[F, (Int, A)], k1: Int, out: Seq[A]): 
        Pull[F, (Int, Seq[A]), Unit] = {

      val differsAt = chunk.indexWhere(_._1 != k1).getOrElse(-1)
      if (differsAt == -1) {
        // The whole chunk matches the current key, add this chunk to the current Stream of chunks
        val newOut: Seq[A] = out ++ chunk.toVector.map(_._2)
        Pull.pure(()) >> go(Some((k1, newOut)))(h)
      } else {
        // at least part of this chunk does not match the current key, I need to group
        // and retain chunkiness
        var startIndex = 0
        var endIndex = differsAt
        // while (differsAt != -1) {
          // I'd like to do chunk.indexWhere(startIndex, k != k1), but I don't have that form of indexWhere
          // I could make it

        // }
        // would it help to turn the chunk into an array?

        // split the chunk into the bit where the keys match and the bit where they don't
        val matching = chunk.take(differsAt)
        val nonMatching = chunk.drop(differsAt)
        // Push the non-matching chunk back to the handle, can I do that?
        // Will it cause upstream side-effects to be executed more than once?
        val newOut: Seq[A] = out ++ matching.toVector.map(_._2)
        // TODO need to find a way to output more than one element at a time here, this is a performance
        // problem
        // I think the solution is to keep processing the whole of the inbound chunk, rather than
        // using h.push
        // But, I'll still need to push the final key of the chunk, to see if it matches up with the
        // start of the next one
        Pull.output1((k1, newOut)) >> go(None)(h.push(nonMatching))
      }
    }

    in => in.pull(go(None))
  }
}
