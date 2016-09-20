package fs2fv

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import cats.data.Xor

import io.circe._
// import io.circe.generic.auto._
import io.circe.jawn._
import io.circe.syntax._

case class RequiredFile(val id: String, val namePattern: String) {

  def nameForDate(date: LocalDate): String =
    DateTimeFormatter.ofPattern(namePattern).format(date)
}

object RequiredFile {

  implicit val decodeRequiredFile: Decoder[RequiredFile] = Decoder.instance(c =>
    for {
      i <- c.downField("id").as[String]
      n <- c.downField("name-pattern").as[String]
    } yield RequiredFile(i, n)
  )

  def parseRequiredFiles(configStr: String): Xor[Error, Set[RequiredFile]] = {
    jawn.decode[Set[RequiredFile]](configStr)
  }
}
