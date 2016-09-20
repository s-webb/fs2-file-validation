package fs2fv

object MergeStreams {

  type Tagged[F] = (Int, F)

  implicit class TaggedOps[F](val self: Tagged[F]) extends AnyVal {
    def tag: Int = self._1
    def record: F = self._2
  }

  object Tagged {
    def apply[F](tag: Int, f: F): Tagged[F] = (tag, f)
  }

  type Record1 = (Int, Seq[String])

  object Record1 {
    def apply(key: Int, values: Seq[String]): Record1 = (key, values)
  }

  implicit class Record1Ops(val self: Record1) extends AnyVal {
    def key: Int = self._1
    def values: Seq[String] = self._2
  }

  type Record2 = (Int, Seq[String], Seq[String])

  def record2ToString(r2: Record2): String =
    s"${r2._1}, 1=${r2._2.mkString(",")}, 2=${r2._3.mkString(",")}"
}
