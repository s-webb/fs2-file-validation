package fs2fv.filevalidation

import fs2fv.{RequiredFile, RowFailure}

import java.nio.file.{Files, Paths}
import java.time.{LocalDate, LocalDateTime}

import doobie.imports._

import org.scalatest.{Matchers, WordSpecLike}

import fs2._

class FileValidationSpec extends WordSpecLike with Matchers {

  import FileValidation._

  "checkFilesExist" should {

    "record no errors if no required files" in {
      val files = Set[String]()
      val required = Set[RequiredFile]()
      val forDate = LocalDate.of(2016, 7, 21)

      val (requiredAt, missing) = checkFilesExist(() => files, required, forDate)

      requiredAt should be (Set[RequiredFile]())
      missing should be (Set[RequiredFile]())
    }

    "record name of each missing file" in {
      val files = Set[String]("file1_2016-07-21.txt")
      val file1 = RequiredFile("file1", "'file1_'yyyy-MM-dd'.txt'")
      val file2 = RequiredFile("file2", "'file2_'yyyy-MM-dd'.txt'")
      val required = Set[RequiredFile](file1, file2)
      val forDate = LocalDate.of(2016, 7, 21)

      val (requiredAt, missing) = checkFilesExist(() => files, required, forDate)

      requiredAt should be (Set[String](
        ("file1_2016-07-21.txt"),
        ("file2_2016-07-21.txt")
      ))

      missing should be (Set[String](
        ("file2_2016-07-21.txt")
      ))
    }
  }

  "countFailuresPipe" should {
    "map from passes and failures to passes and count" in {

      val s: Stream[Nothing, Either[RowFailure, Seq[String]]] = Stream(
        Right(Seq[String]("a1", "b1", "c1")),
        Left(RowFailure(1, "msg")),
        Left(RowFailure(2, "msg")),
        Right(Seq[String]("a2", "b2", "c2")),
        Left(RowFailure(3, "msg")),
        Right(Seq[String]("a3", "b3", "c3"))
      )

      val o: Seq[((Int, Int), Seq[String])] = s.throughPure(StreamOps.countsAndPasses).toVector
      o should equal (Vector(
        ((0, 1), Seq[String]("a1", "b1", "c1")),
        ((2, 2), Seq[String]("a2", "b2", "c2")),
        ((3, 3), Seq[String]("a3", "b3", "c3"))
      ))
    }
  }
}
