import org.scalameter.api._

import fs2._
import fs2fv.MergeStreams._

object MergeStreamsBenchmark extends Bench.LocalTime {

  val ops = new GroupOps[(Int, Seq[String]), String, Int] {
    def emptyGroup(k: Int): (Int, Seq[String]) = (k, Seq[String]())
    def keyOf(a: (Int, Seq[String])): Int = a._1
    def outputFor(k: Int, as: Map[Int, Tagged[(Int, Seq[String])]]): String = {
      val l = as(1)._2._2.mkString(",")
      val r = as(2)._2._2.mkString(",")
      s"$l ++ $r"
    }
    def ordering: Ordering[Int] = implicitly[Ordering[Int]]
  }

  val tags = Set(1, 2)
  val numTokensPerKey = 10

  val numKeysPerStream = Gen.range("numKeysPerStream")(10000, 30000, 10000)
  val buffs: Gen[Buff[(Int, Seq[String])]] = for {
    keysPerStream <- numKeysPerStream
  } yield {
    tags.map { tag =>
      val records: Seq[(Int, (Int, Seq[String]))] = (0 until keysPerStream).map { key =>
        val tokens: Seq[String] = (0 until numTokensPerKey).map { token =>
          s"$tag.$key.$token"
        }
        (tag, (key, tokens))
      }
      (tag, records)
    }.toMap
  }

  performance of "createOutputChecked" in {
    measure method "createOutputChecked" in {
      using(buffs) in {
        buff => {
          createOutputChecked[(Int, Seq[String]), String, Int](tags, buff)(ops)
        }
      }
    }
  }
}
