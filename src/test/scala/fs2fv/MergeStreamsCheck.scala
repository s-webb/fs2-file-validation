package fs2fv

import com.typesafe.scalalogging.LazyLogging

import fs2fv.MergeStreams._

import org.junit.Test

import org.scalacheck._
import org.scalatest.Matchers
import org.scalatest.junit.JUnitSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.collection.JavaConverters._

class MergeStreamsCheck extends JUnitSuite with GeneratorDrivenPropertyChecks with Matchers with LazyLogging {

  @Test
  def testCreateOutput(): Unit = {
    forAll(genBuffs) { buff =>

      val tagList = buff.keySet.toSeq.sorted
      val buffInfo = tagList.flatMap(t => buffInfoFor(t, buff))

      logger.debug(s"Tags: ${tagList.mkString(",")}")
      buffInfo.foreach(bi => logger.debug("InBuff: " + bi.printKeys))

      val lowestMaxKey = buffInfo.sortBy(_.max).head
      logger.debug(s"Lowest max key is: ${lowestMaxKey.printInfo}")

      val out = createOutputChecked(tagList.toSet, buff)(ops)

      if (out.isDefined) {
        val (outRecs, outBuff) = out.get
        val outBuffInfo = tagList.flatMap(t => buffInfoFor(t, outBuff))
        logger.debug(s"Num output records: ${outRecs.size}")
        outRecs.foreach(r => logger.debug(s"Out: ${r}"))
        outBuffInfo.foreach(bi => logger.debug("OutBuff: " + bi.printKeys))

  // what properties should hold for this function?
  //  - all elements present in the input should be present in either the output or the remainder
  //  - there should be no empty tag buffers in the remainder (unless there was an empty one in the input)
  //  - if there was an empty tag buffer in the input, the output should be None
  //  - if all of the input buffers are non-empty, the output should be Some
  //  - all records with a key <= the lowest max should be present in the output (and not in the rem)
  //  - all records with a key > the lowest max should be present in the rem (and not in the output)

        // the lowest max key should be the final key in the output
        outRecs.last._1 should be (lowestMaxKey.max)

        // any keys left in the output buff should be greater than the highest key in output
        outBuffInfo.foreach { ob =>
          ob.min should be > lowestMaxKey.max
        }

        // at least one of the input buffers should have been fully consumed
        outBuffInfo.size should be < buffInfo.size

        // all of the input should either be in the output or remainder
        val outRecsKeys: Seq[(Int, Int)] = outRecs.flatMap { case (k, tags) => tags.map(t => (t, k)) }
        val outBuffKeys: Seq[(Int, Int)] = outBuffInfo.flatMap(bi => bi.keys.map(k => (bi.tag, k)))
        val outKeys = (outRecsKeys ++ outBuffKeys).sorted
        val inKeys: Seq[(Int, Int)] = (buffInfo.flatMap(bi => bi.keys.map(k => (bi.tag, k)))).sorted

        outKeys.foreach { ok =>
          logger.debug(s"Out key: $ok")
        }
        inKeys.foreach { ik =>
          logger.debug(s"In key: $ik")
        }
        outKeys should be (inKeys)
      }
    }
  }

  def buffInfoFor[A](tag: Int, buff: Buff[(Int, A)]): Option[BuffInfo] = {
    buff.get(tag).flatMap { tagBuffer =>
      if (tagBuffer.isEmpty) None else {
        val max = tagBuffer.last._2._1
        val min = tagBuffer.head._2._1
        val count = tagBuffer.size
        val keys = tagBuffer.map(r => r._2._1)
        Some(BuffInfo(tag, min, max, count, keys))
      }
    }
  }

  // to generate keys, just pick numbers from 0 to 100 and then sort them
  def recordSeq(tag: Int): Gen[Seq[(Int, (Int, Seq[String]))]] = 
    for {
      numKeys <- Gen.choose(1, 10)
      keys <- Gen.listOfN(numKeys, Gen.choose(0, 10))
    } yield {
      Set(keys:_*).toSeq.sorted.map(k => (tag, (k, Seq[String]("A"))))
    }

  def mkBuffs(count: Int): Gen[Buff[(Int, Seq[String])]] = {
    val tbs: Seq[Gen[(Int, Seq[(Int, (Int, Seq[String]))])]] = 
      (0 to count).map { n =>
        val t = n + 1
        recordSeq(t).map(s => (t, s))
      }
    val gtbs: Gen[Seq[(Int, Seq[(Int, (Int, Seq[String]))])]] = Gen.sequence(tbs).map(_.asScala)
    gtbs.map(_.toMap)
  }

  val genBuffs: Gen[Buff[(Int, Seq[String])]] = 
    for {
      numTags <- Gen.choose(1, 3)
      buff <- mkBuffs(numTags)
    } yield buff 

  case class BuffInfo(tag: Int, min: Int, max: Int, count: Int, keys: Seq[Int]) {
    def printInfo: String = s"[t: $tag, min: $min, max: $max, count: $count]"
    def printKeys: String = s"[t: $tag, keys: ${keys.mkString(", ")}]"
  }

  val ops = new GroupOps[(Int, Seq[String]), (Int, Seq[Int]), Int] {
    def emptyGroup(k: Int): (Int, Seq[String]) = (k, Seq[String]())
    def keyOf(a: (Int, Seq[String])): Int = a._1
    def outputFor(k: Int, as: Map[Int, Tagged[(Int, Seq[String])]]): (Int, Seq[Int]) = {
      // output is the key and the list of tags that had non-zero output for that key
      val nonEmptyTags = as.values.flatMap { case (tag, (key, tokens)) => 
          if (tokens.isEmpty) None else Some(tag)
        }.toSeq.sorted
      (k, nonEmptyTags)
    }
    def ordering: Ordering[Int] = implicitly[Ordering[Int]]
  }
}
