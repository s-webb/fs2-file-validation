package fs2fv.streamthings

import fs2._
import fs2.util.{Async, Suspendable}

import fs2fv.GroupKeys._
import fs2fv.MergeStreams._

import java.nio.file.{Path, Paths}

object StreamThings {

  val fileChunkSizeBytes = 100 * 1024

  type RowFailure = (Int, String)
  type TokenizedLine = (Array[String], Int)
  type Counts = (Int, Int)
  type CountsAndLine = (Counts, TokenizedLine)
  type KeyedLine = (Int, CountsAndLine)
  type KeyedLineGroup = (Int, Seq[CountsAndLine])
  type OutputRecord = (Int, Seq[Seq[CountsAndLine]])

  type MkByteStream[F[_]] = () => Stream[F, Byte]

  implicit val groupOps = new GroupOps[KeyedLineGroup, OutputRecord, Int] {
    def emptyGroup(k: Int): KeyedLineGroup = (k, Seq[CountsAndLine]())
    def keyOf(a: KeyedLineGroup): Int = a._1

    def outputFor(k: Int, as: Map[Int, Tagged[KeyedLineGroup]]): OutputRecord = {
      (k, as.keys.toSeq.sorted.map(t => as(t).record._2))
    }
    def ordering: Ordering[Int] = implicitly[Ordering[Int]]
  }

  /**
   * Validate a list of files, merge and write the contents and return a count of invalid rows for each input file.
   *
   * Errors are sent to the given failure sink where they can be observed/recorded (e.g. to a db).
   *
   * The merge operation extracts a key from each line of input, groups together records from each input file by key,
   * then gathers together one group from each of the input files to be output together.
   *
   * This is intended as the main entry point to the program.
   */
  def validateAndMerge[F[_]](
      inFilenames: Seq[String], 
      outFilename: String)(implicit 
      ev1: Suspendable[F], 
      ev2: Async[F]): Stream[F, Seq[Int]] = {

    // TODO - only question left here is how to construct the failure sink
    val failureSink: Sink[F, RowFailure] = ???
    val bytesOut: Sink[F, Byte] = writeOutput(Paths.get(outFilename))
    val bytesIn: Seq[MkByteStream[F]] = inFilenames.map { n => () => readBytes[F](Paths.get(n)) }

    validateAndMergeStreams(failureSink, bytesOut)(bytesIn)
  }

  /**
   * Similar to validateAndMerge, but allows various dependencies to be passed in (for testing).
   */
  def validateAndMergeStreams[F[_]](
      failureSink: Sink[F, RowFailure],
      bytesOut: Sink[F, Byte])(
      bytesIn: Seq[MkByteStream[F]])(implicit 
      ev1: Suspendable[F], 
      ev2: Async[F]): Stream[F, Seq[Int]] = {

    val outputSink: Sink[F, OutputRecord] = _.through(outputPipe).to(bytesOut)

    collateByKey(bytesIn.map(_().through(validRecords(failureSink)))).
      observe(outputSink).
      through(countErrors(bytesIn.size))
  }

  def countErrors[F[_]](numInputs: Int): Pipe[F, OutputRecord, Seq[Int]] = in => {
    val errCounts: Seq[Int] = (0 to numInputs).map(_ => 0)
    in.fold(errCounts) { case (acc, (_, rs)) =>
      val rsErrCounts: Seq[Int] = rs.map(maxForGroup)
      takeHighest(acc, rsErrCounts)
    }
  }

  private [streamthings] def maxForGroup(group: Seq[CountsAndLine]): Int = 
    group.lastOption.map { case ((f, _), _) => f }.getOrElse(0)

  private [streamthings] def takeHighest(l1: Seq[Int], l2: Seq[Int]): Seq[Int] =
    l1.zip(l2).map { case (a, b) => a max b }

  def outputPipe[F[_]]: Pipe[F, OutputRecord, Byte] =
    _.through(toOutputLines).
      intersperse("\n").
      through(text.utf8Encode[F])


  def validRecords[F[_]: Async](failureSink: Sink[F, RowFailure]): Pipe[F, Byte, KeyedLineGroup] = 
    _.through(toLines).
      through(validate(failureSink)).
      through(extractKey).
      through(groupKeys)

  def toLines[F[_]]: Pipe[F, Byte, TokenizedLine] = 
    _.through(text.utf8Decode).
      through(text.lines).
      map(_.split('|')).
      zipWithIndex

  def validate[F[_]: Async](failureSink: Sink[F, RowFailure]): Pipe[F, TokenizedLine, CountsAndLine] = in => {
    in.through(rowValidator).
      observe(adaptSink(failureSink)).
      through(countsAndPasses).
      takeWhile { case (cs, _) => !tooManyErrors(cs) }
  }

  def adaptSink[F[_]](failureSink: Sink[F, RowFailure]): Sink[F, Either[RowFailure, TokenizedLine]] = 
    _.collect { case Left(f) => f }.to(failureSink)

  def rowValidator[F[_]]: Pipe[F, TokenizedLine, Either[RowFailure, TokenizedLine]] = 
    _.map { case ln@(tokens, lineNum) =>
      if (tokens.size == 4) {
        Right(ln)
      } else {
        Left((lineNum, "Wrong number of tokens"))
      }
    }

  def tooManyErrors(cs: Counts): Boolean = {
    val (f, p) = cs
    val total = f + p
    // where 'too high' is >= 50% after 10000 records
    (total > 10000) && ((f * 100) / total) > 50
  }

  def countsAndPasses[F[_]]: Pipe[F, Either[RowFailure, TokenizedLine], CountsAndLine] = in => {
    val noLine: TokenizedLine = (Array[String](), -1)
    val z: CountsAndLine = ((0, 0), noLine)
    val counted = in.scan (z) { 
      case (((f, p), _), Left(failure)) => ((f + 1, p), noLine)
      case (((f, p), _), Right(pass)) => ((f, p + 1), pass)
    }
    counted.filter(_._2 != noLine)
  }

  /**
   * Construct a key for the line (by parsing the first token as an int)
   */
  def extractKey[F[_]]: Pipe[F, CountsAndLine, KeyedLine] = 
    _.map { case r@(cs, line) => (line._1(0).toInt, r) }

  /**
   * Ordering of the streams in 'ins' is significant, it affects ordering of values in the OutputRecord tuple.
   */
  def collateByKey[F[_]](ins: Seq[Stream[F, KeyedLineGroup]])(implicit ev1: Async[F]): Stream[F, OutputRecord] = {
    require(ins.size == 2)
    val withIndex = ins.zipWithIndex
    val tagged: Seq[Stream[F, Tagged[KeyedLineGroup]]] =
      withIndex.map { case (s, i) => s.map((i, _)) }
    val tags: Set[Int] = withIndex.map(_._2).toSet
    val merged: Stream[F, Tagged[KeyedLineGroup]] = tagged.reduce(_ merge _)
    merged.through(joinTagged[F, KeyedLineGroup, OutputRecord, Int](tags))
  }

  def toOutputLines[F[_]]: Pipe[F, OutputRecord, String] = 
    _.flatMap { case (key, ls) =>
      Stream(Seq(key.toString) ++ ls.flatten.map(countsAndLineToStr):_*)
    }

  def countsAndLineToStr(countsAndLine: CountsAndLine): String = {
    val (_, (ln, _)) = countsAndLine
    ln.mkString("|")
  }

  def readBytes[F[_]: Suspendable](file: Path): Stream[F, Byte] =
    io.file.readAll[F](file, fileChunkSizeBytes)

  def writeOutput[F[_]: Suspendable](file: Path): Sink[F, Byte] =
    io.file.writeAll(file)
}
