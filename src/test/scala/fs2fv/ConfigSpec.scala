package fs2fv

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import java.util.regex.Pattern

import org.scalatest.{Matchers, WordSpecLike}

class ConfigSpec extends WordSpecLike with Matchers {

  val configString = """{
      |  "output" : [
      |    {
      |      "name" : "merge.csv",
      |      "inputs" : [
      |        {
      |          "name" : "merge_a.csv",
      |          "columns" : [
      |            {
      |              "name" : "a_col_a",
      |              "pattern" : ".{1,10}"
      |            }
      |          ]
      |        },
      |        {
      |          "name" : "merge_b.csv",
      |          "columns" : [
      |            {
      |              "name" : "b_col_a",
      |              "pattern" : ".{0,5}"
      |            }
      |          ]
      |        }
      |      ]
      |    }
      |  ]
      |}""".stripMargin

  "parseConfig" should {
    "parse a json string" in {
      val c = Config(List(
        Output("merge.csv", List(
          Input("merge_a.csv", List(
              Column("a_col_a", ".{1,10}")
          )),
          Input("merge_b.csv", List(
            Column("b_col_a", ".{0,5}")
          ))
        ))
      ))
      val cs: String = c.asJson.spaces2
      cs should be (configString)
      println(cs)
      val cp: Either[Error, Config] = decode[Config](cs)
      cp should be (Right(c))
    }
  }
}
