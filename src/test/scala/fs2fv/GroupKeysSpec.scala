package fs2fv

import fs2._

/*
 * groupKeys1 - takes a Stream[A, (Int, String)] and produces a Stream[A, (Int, Stream[A, String]])],
 * grouping entries from the initial stream by their Int key into a nested Stream
 *
 * groupKeys2 - parameterized version of groupKeys
 *
 * groupKeys3 - not parameterized, uses a Seq instead of a Stream to store each group. Why did I 
 * switch from Stream to Seq for the groups? Is it to make the client code simpler?
 *
 * groupKeys - to be imported by client code, points to the version of group keys that should be used
 */
object GroupKeys {
  
  def groupKeys1[F[_]]: Pipe[F, (Int, String), Stream[F, (Int, String)]] = {

    def go(current: Option[(Int, Stream[F, (Int, String)])]): 
        Handle[F, (Int, String)] => Pull[F, Stream[F, (Int, String)], Unit] = h => {

      // this is great and it works and everything, but I think you can do the same thing with Handle.takeWhile :-)
      current match {
        case None =>
          // if we don't currently have a key, set the key to that of the first record and recurse
          h.receive { (chunk, _) => 
            // what does the first element of the chunk look like?
            if (chunk.isEmpty) {
              // empty chunk and we have no key, done
              Pull.done
            } else {
              // non-empty chunk and we have no key, recurse using key from first value
              // applying the recursive step to the original h because we haven't taken
              // anything from it yet
              val newKey = chunk(0)._1
              Pull.pure(()) >> go(Some(
                (newKey, Stream[F, (Int, String)]())
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
                val newOut: Stream[F, (Int, String)] = out ++ Stream.chunk(chunk)
                Pull.pure(()) >> go(Some(
                  (k1, newOut)
                ))(h)
              } else {
                // split the chunk into the bit where the keys match and the bit where they don't
                val matching = chunk.take(differsAt)
                val nonMatching = chunk.drop(differsAt)
                // Push the non-matching chunk back to the handle, can I do that?
                val newOut: Stream[F, (Int, String)] = out ++ Stream.chunk(matching)
                Pull.output1(newOut) >> go(None)(h.push(nonMatching))
              }

            case None =>
              Pull.output1(out) >> Pull.done
          }
      }
    }
    in => in.pull(go(None))
  }

  def groupKeys2[F[_], A, B](aToB: A => B, predicate: (A, B) => Boolean): Pipe[F, A, Stream[F, A]] = {

    def go(current: Option[(B, Stream[F, A])]): 
        Handle[F, A] => Pull[F, Stream[F, A], Unit] = h => {

      current match {
        case None =>
          h.receive { (chunk, _) => 
            if (chunk.isEmpty) {
              Pull.done
            } else {
              val newKey = aToB(chunk(0))
              val newCurrent = Some((newKey, Stream[F, A]()))
              Pull.pure(()) >> go(newCurrent)(h)
            }
          }

        case Some((k1, out)) =>
          h.receiveOption { 
            case Some((chunk, h)) => 
              val differsAt = chunk.indexWhere(a => !predicate(a, k1)).getOrElse(-1)
              if (differsAt == -1) {
                val newOut: Stream[F, A] = out ++ Stream.chunk(chunk)
                val newCurrent = Some((k1, newOut))
                Pull.pure(()) >> go(newCurrent)(h)
              } else {
                val matching = chunk.take(differsAt)
                val nonMatching = chunk.drop(differsAt)
                val newOut: Stream[F, A] = out ++ Stream.chunk(matching)
                Pull.output1(newOut) >> go(None)(h.push(nonMatching))
              }

            case None =>
              Pull.output1(out) >> Pull.done
          }
      }
    }
    in => in.pull(go(None))
  }

  /**
   * Pipe that groups adjacent elements in a stream by key, where each element in the output stream contains a Seq
   * of multiple elements from the input stream
   */
  def groupKeys3[F[_]]: Pipe[F, (Int, String), (Int, Seq[String])] = {

    def go(current: Option[(Int, Seq[String])]): 
        Handle[F, (Int, String)] => Pull[F, (Int, Seq[String]), Unit] = h => {

      current match {
        case None =>
          h.receive { (chunk, _) => 
            if (chunk.isEmpty) {
              Pull.done
            } else {
              val newKey = chunk(0)._1
              Pull.pure(()) >> go(Some(
                (newKey, Seq[String]())
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
                val newOut: Seq[String] = out ++ chunk.toVector.map(_._2)
                Pull.pure(()) >> go(Some(
                  (k1, newOut)
                ))(h)
              } else {
                // split the chunk into the bit where the keys match and the bit where they don't
                val matching = chunk.take(differsAt)
                val nonMatching = chunk.drop(differsAt)
                // Push the non-matching chunk back to the handle, can I do that?
                val newOut: Seq[String] = out ++ matching.toVector.map(_._2)
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
