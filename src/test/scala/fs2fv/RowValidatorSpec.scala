package fs2fv

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import fs2._
import fs2fv.RowValidator._
import fs2fv.ValidateAndMerge.{TokenizedLine, RowFailure}

import org.scalatest._

class RowValidatorSpec extends WordSpecLike with Matchers {

  "rowValidator" should {
    "reject line that has wrong number of tokens" in {
      val configStr = """
        {
          "name" : "records-a-2016-07-21.txt",
          "columns" : [
            {
              "name" : "a_col_a",
              "pattern": ".{3}"
            },
            {
              "name" : "a_col_b",
              "pattern": ".{3}"
            }
          ]
        }""".stripMargin
      val configE: Either[Error, Input] = decode[Input](configStr)
      val config: Input = configE.right.get

      val input = ("""abc""".stripMargin.split("\\|"), 0)
      val s: Stream[Nothing, TokenizedLine] = Stream(input)
      val o: Seq[Either[RowFailure, TokenizedLine]] = 
          s.throughPure(rowValidator(config)).toList
      // asserting on this output is tricky...
      val o0 = o(0).left.get
      o0._1._1 should be (Array("abc"))
      o0._1._2 should be (0)
      o0._2 should be (Seq("expected 2 tokens, got 1"))
    }
    "reject line that has invalid token" in {
      val configStr = """
        {
          "name" : "records-a-2016-07-21.txt",
          "columns" : [
            {
              "name" : "a_col_a",
              "pattern": "[^A-D]{1}"
            }
          ]
        }""".stripMargin
      val configE: Either[Error, Input] = decode[Input](configStr)
      val config: Input = configE.right.get

      val input = ("""A""".stripMargin.split("\\|"), 0)
      val s: Stream[Nothing, TokenizedLine] = Stream(input)
      val o: Seq[Either[RowFailure, TokenizedLine]] = 
          s.throughPure(rowValidator(config)).toList
      // asserting on this output is tricky...
      val o0 = o(0).left.get
      o0._1._1 should be (Array("A"))
      o0._1._2 should be (0)
      o0._2 should be (Seq("token 0, 'A', does not match pattern [^A-D]{1}"))
    }
  }
}
