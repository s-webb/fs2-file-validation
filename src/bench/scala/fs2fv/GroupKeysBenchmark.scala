import org.scalameter.api._

import fs2._


object GroupKeysBenchmark extends Bench.LocalTime {

  val sizes = Gen.range("size")(1000, 5000, 1000)

  val streams = for {
    size <- sizes
  } yield {
    // TODO generate a stream of the given size
    val ns = (0 until size).map(n => (n / 10, n.toString))
    Stream[Task, (Int, String)](
      ns:_*
    ).rechunkN(100)
  }


  performance of "groupKeys" in {
    measure method "new" in {
      import fs2fv.GroupKeys._
      using(streams) in {
        s => {
          s.through(groupKeys).run.unsafeRun
        }
      }
    }
    measure method "old" in {
      import fs2fv.GroupKeysOrig._
      using(streams) in {
        s => {
          s.through(groupKeys).run.unsafeRun
        }
      }
    }
  }
}
