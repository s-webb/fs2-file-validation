package fs2fv

import fs2._

import org.scalatest.{Matchers, WordSpecLike}

class SignalExampleSpec extends WordSpecLike with Matchers {

  "signal" should {
    "stop processing" in {
      implicit val S = fs2.Strategy.fromFixedDaemonPool(2, threadName = "worker")
      val s = Stream.eval(async.signalOf[Task, Int](0)).flatMap { s =>
        val control: Stream[Task, Boolean] = s.discrete.map(_ >= 5)
        val data: Stream[Nothing, Int] = Stream.range(0, 10)
        val writer: Stream[Task, Int] = data.evalMap { d => s.set(d).map(_ => d) }
        writer interruptWhen control
      }
      s.runLog.unsafeRun should be (Vector(0, 1, 2, 3, 4, 5))
    }
  }
}
