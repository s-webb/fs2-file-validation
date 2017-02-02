package fs2fv

import fs2._

import fs2fv.GroupKeys._
import org.scalatest.{Matchers, WordSpecLike}

class GroupKeysSpec extends WordSpecLike with Matchers {

  "groupKeys" should {
    "preserve chunkiness" in {
      val s = Stream((1, "A"), (1, "B"), (2, "C"), (3, "D"))
      val numInChunks = s.chunks.toVector.size
      val s2 = s.throughPure(groupKeys)
      val numOutChunks = s2.chunks.toVector.size
      numOutChunks should be <= (numInChunks + 1)
      s2.toVector should be (Vector((1, Seq("A", "B")), (2, Seq("C")), (3, Seq("D"))))
    }
  }

  "groupBy" should {
    "preserve chunkiness" in {
      val s = Stream((1, "A"), (1, "B"), (2, "C"), (3, "D"))
      val numInChunks = s.chunks.toVector.size
      val s2 = s.groupBy(_._1)
      // val s2 = s.throughPure(GroupBy.groupBy(_._1))
      val numOutChunks = s2.chunks.toVector.size
      numOutChunks should be <= (numInChunks + 1)
      s2.toVector should be (Vector((1, Vector((1, "A"), (1, "B"))), (2, Vector((2, "C"))), (3, Vector((3, "D")))))

    }
  }
}
