package fs2fv

import java.time.LocalDate

import cats.data.Xor

import org.scalatest.{Matchers, WordSpecLike}

class RequiredFileSpec extends WordSpecLike with Matchers {

  import RequiredFile._

  "nameForDate" should {
    "generate filename at given date" in {
      val rf = RequiredFile("f", "'name-'yyyy-MM-dd'.txt'")
      val d = LocalDate.of(2016, 7, 21)
      rf.nameForDate(d) should be ("name-2016-07-21.txt")
    }
  }

  "parseRequiredFiles" should {
    "parse required files from config" in {
      val config = """[
        |  {
        |    "id": "records-a", 
        |    "name-pattern": "records-a.txt"
        |  },
        |  { 
        |    "id": "records-b",
        |    "name-pattern": "records-b.txt"
        |  }
        |]""".stripMargin

        parseRequiredFiles(config) should be (Xor.Right(
          Set(
            RequiredFile("records-a", "records-a.txt"),
            RequiredFile("records-b", "records-b.txt")
          )
        ))
    }
  }
}
