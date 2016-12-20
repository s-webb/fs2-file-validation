import org.scalameter.api._

import fs2._

import fs2fv.GroupKeys._

object RangeBenchmark extends Bench.LocalTime {

  val sizes = Gen.range("size")(1000, 5000, 1000)

  val ranges = for {
    size <- sizes
  } yield {
    // TODO generate a stream of the given size
    val ns = (0 until size).map(n => (n / 10, n.toString))
    Stream[Task, (Int, String)](
      ns:_*
    )
  }


  performance of "groupKeys" in {
    measure method "map" in {
      using(ranges) in {
        s => {
          s.through(groupKeys).run.unsafeRun
        }
      }
    }
  }
}
