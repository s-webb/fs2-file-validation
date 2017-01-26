package fs2fv

import fs2._
import fs2fv.GroupKeys._
import org.junit.Test
import org.scalacheck._
import org.scalacheck.Prop.forAll
import org.scalatest.Matchers
import org.scalatest.junit.JUnitSuite
import org.scalatest.prop.{Checkers, GeneratorDrivenPropertyChecks}

class GroupKeysCheck extends JUnitSuite with GeneratorDrivenPropertyChecks with Matchers {

  @Test
  def testChunkiness(): Unit = {
    forAll(gen) { s1 =>
      val inChunks = s1.chunks.toVector
      val s2 = s1.throughPure(groupKeys)
      val outChunks = s2.chunks.toVector
      val numInChunks = inChunks.size
      val numOutChunks = s2.chunks.toVector.size
      val inValues: Vector[(Int, String)] = inChunks.map(_.toVector).flatten
      val outValues: Vector[(Int, String)] = outChunks.flatMap{_.toVector.flatMap{ case (k, vs) => vs.map(v => (k, v))}}
      numOutChunks should be <= (numInChunks + 1)
      inValues should be (outValues)
    }
  }

  val gen: Gen[Stream[Nothing, (Int, String)]] = {
    // generate a set of ints in range 0 .. 10, these will be key increments
    // for each key increment, generate an int in range 0..50, these will be the the number of values per key
    val incAndCount: Gen[(Int, Int)] =
      for {
        increment <- Gen.choose(0, 5)
        count <- Gen.choose(0, 10)
      } yield (increment, count)

    val values: Gen[Seq[(Int, String)]] = for {
      numKeyIncrements <- Gen.choose(0, 30)
      iAndC <- Gen.listOfN(numKeyIncrements, incAndCount)
    } yield {
      iAndC.foldLeft((0, Seq[(Int, String)]())) { (acc: (Int, Seq[(Int, String)]), v) =>
        val (inc, count) = v
        val k = acc._1 + inc
        (k, acc._2 ++ Seq.fill(count)((k, "A")))
      }._2
    }

    val chunks: Gen[Seq[Seq[(Int, String)]]] = for {
      vs <- values
      numChunks <- Gen.choose(3, 10)
      chunkSizes <- Gen.listOfN(numChunks, Gen.choose(0, 20))
    } yield {
      chunkSizes.foldLeft((vs, Seq[Seq[(Int, String)]]())) { (accT, v) =>
        val (vs, acc) = accT
        if (vs.isEmpty) {
          (vs, acc)
        } else {
          if (vs.size > v) {
            val (left, right) = vs.splitAt(v)
            (right, acc :+ left)
          } else {
            (Seq.empty, acc :+ vs)
          }
        }
      }._2
    }

    chunks.map(_.foldLeft(Stream[Nothing, (Int, String)]()) { (acc, c) =>
      acc.append(Stream.emits(c))
    })
  }
}
