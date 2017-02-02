package fs2fv

import java.nio.file.Paths
import java.io.{BufferedWriter, FileWriter, PrintWriter}

import scala.util.Random

object TestData {

  val rnd = new Random()

  def randomAlphaChar: Char = {
    val n = rnd.nextInt('Z' - 'A')
    ('A' + n).toChar
  }

  def valid: Boolean = {
    // 5% error rate
    rnd.nextInt(100) < 95
  }

  def makeRow(n: Int): String = {
    if (valid) {
      s"$n|${randomAlphaChar}|${randomAlphaChar}"
    } else {
      s"$n|${randomAlphaChar}|${randomAlphaChar}|${randomAlphaChar}"
    }
  }

  def makeTestData(p: String, maxId: Int): Unit = {
    val fw = new FileWriter(Paths.get(p).toFile)
    val writer = new PrintWriter(new BufferedWriter(fw))
    (1 to maxId).foreach { n =>
      val numRows = rnd.nextInt(10)
      (0 until numRows).foreach { _ =>
        writer.println(makeRow(n))
      }
    }
    writer.flush()
    writer.close()
  }
}
