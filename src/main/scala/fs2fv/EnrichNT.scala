package fs2fv

import scalaz._
import Scalaz._

/**
 * Stolen from Pawel Szulc's "Make Your Programs Free" talk at Scalaworld 2016,
 * he, in turn, took it from Quasar Analytics.
 *
 * All I know is that it allows me to compose interpreters.
 *
 * Voodoo.
 */
object EnrichNTOps {

  sealed abstract class :+:[F[_], G[_]] {
    type λ[A] = Coproduct[F, G, A]
  }

  implicit class EnrichNT[F[_], H[_]](f: F ~> H) {
    def :+:[G[_]](g: G ~> H): (G :+: F)#λ ~> H = new ((G :+: F)#λ ~> H) {
      def apply[A](fa: (G :+: F)#λ[A]) = fa.run.fold(g, f)
    }
  }
}

