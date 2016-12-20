package fs2fv

import fs2._

import org.scalatest.{Matchers, WordSpecLike}

class GroupKeysSpec extends WordSpecLike with Matchers {

  "groupKeys" should {
    "preserve chunking over key boundaries" ignore {
      import GroupKeys._
      val s = Stream(
        (1, "a"),
        (1, "b"),
        (2, "c")
      )
      s.throughPure(groupKeys).chunks.toList.size should be (1)
    }
  }
}
