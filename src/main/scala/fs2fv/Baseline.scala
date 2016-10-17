package fs2fv

import java.io._
import java.time.{Duration, LocalDate, LocalDateTime}

import com.typesafe.scalalogging.StrictLogging

/**
 * Uses java.io APIs to iterate through contents of a file by line, using String.split to tokenize each line, to
 * give some idea of 'best' performance that can be achieved using Java to process a file on a given machine.
 */
object Baseline extends StrictLogging {

  def run() {

    val startedAt = LocalDateTime.now
    val file = new File("it/large/staging/records-a-2016-07-21.txt")
    val reader = new BufferedReader(new FileReader(file))
    var ln = reader.readLine()
    var lineCount = 0
    var badLines = 0
    while (ln != null) {
      val tokens = ln.split("\\|")
      if (tokens.length != 3) badLines += 1
      lineCount += 1
      ln = reader.readLine()
    }
    reader.close()

    val endedAt = LocalDateTime.now
    val duration = Duration.between(startedAt, endedAt)
    logger.info(s"Bad line count: $badLines")
    logger.info(s"Run completed in duration of: $duration")
  }
}
