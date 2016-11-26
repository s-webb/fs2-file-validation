package fs2fv

import MergeStreams._

object Records {

  implicit val recordGroupOps = new GroupOps[Record1, Record2, Int] {
    def emptyGroup(key: Int): Record1 = Record1(key, Seq[String]())
    def keyOf(a: Record1): Int = a.key
    def outputFor(k: Int, as: Map[Int, Tagged[Record1]]): Record2 = 
      (k, as(1).record.values, as(2).record.values)
    def ordering: Ordering[Int] = implicitly[Ordering[Int]]
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
