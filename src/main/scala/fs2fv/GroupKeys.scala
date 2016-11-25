package fs2fv

import fs2._

object GroupKeys {

  /**
   * Pipe that groups adjacent elements in a stream by key, where each element in the output stream contains a Seq
   * of multiple elements from the input stream
   */
  def groupKeys[F[_], A]: Pipe[F, (Int, A), (Int, Seq[A])] = {

    def go(current: Option[(Int, Seq[A])]): 
        Handle[F, (Int, A)] => Pull[F, (Int, Seq[A]), Unit] = h => {

      current match {
        case None =>
          h.receive { (chunk, _) => 
            if (chunk.isEmpty) {
              Pull.done
            } else {
              val newKey = chunk(0)._1
              Pull.pure(()) >> go(Some(
                (newKey, Seq[A]())
              ))(h)
            }
          }

        case Some((k1, out)) =>
          // what if h is empty?
          h.receiveOption { 
            case Some((chunk, h)) => 
              // take from the chunk while the keys match
              val differsAt = chunk.indexWhere(_._1 != k1).getOrElse(-1)
              if (differsAt == -1) {
                // Add this chunk to the current Stream of chunks
                val newOut: Seq[A] = out ++ chunk.toVector.map(_._2)
                Pull.pure(()) >> go(Some(
                  (k1, newOut)
                ))(h)
              } else {
                // split the chunk into the bit where the keys match and the bit where they don't
                val matching = chunk.take(differsAt)
                val nonMatching = chunk.drop(differsAt)
                // Push the non-matching chunk back to the handle, can I do that?
                val newOut: Seq[A] = out ++ matching.toVector.map(_._2)
                Pull.output1((k1, newOut)) >> go(None)(h.push(nonMatching))
              }

            case None =>
              Pull.output1((k1, out)) >> Pull.done
          }
      }
    }
    in => in.pull(go(None))
  }
}
