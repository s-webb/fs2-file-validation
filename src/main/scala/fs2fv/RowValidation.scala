package fs2fv

import java.nio.file.Paths

import scala.language.higherKinds

import doobie.imports._

import fs2.{Pipe, Pure, Stream}
import fs2.{io, text, Task}
import fs2.util._

import com.typesafe.scalalogging.StrictLogging

object RowValidation extends StrictLogging {

  def validateBytes[F[_]](bytes: Stream[F, Byte], identifier: Option[String] = None): Stream[F, RowFailure] = {
    val lines: Stream[F, String] = bytes.through(text.utf8Decode).through(text.lines)
    val indexed: Stream[F, (String, Int)] = lines.zipWithIndex
    val noBlanks = indexed.filter(t => !t._1.isEmpty)
    val tokens: Stream[F, (Seq[String], Int)] = noBlanks.map { case (l, n) => (l.split("\\|"), n) }
    val badLines: Stream[F, (Seq[String], Int)] = tokens.filter { case (ts, n) =>
      logger.debug(s"${identifier.map(_ + ": ").getOrElse("")}Validating line $n")
      ts.size != 3 
    }
    badLines.map((lineToFailure _).tupled)
  }

  def failuresToInserts[F[_]](jobFileId: Int, failures: Stream[F, RowFailure]): Stream[F, ConnectionIO[Int]] = {
    failures.map(f => DbAccess.failureToInsert(jobFileId)(f))
  }

  def lineToFailure(tokens: Seq[String], n: Int): RowFailure =
    RowFailure(n, s"found ${tokens.size} tokens, expected 3")
}

case class RowFailure(rowNum: Int, message: String)
