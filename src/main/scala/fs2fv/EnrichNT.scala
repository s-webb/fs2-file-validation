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
    // calling this type 'lambda' is confusing to me, a 'type lamba' in scala looks similar
    // to this but is actually an anonymous type declaration where you give a name to a member
    // of the anonymous type so that you can use it elsewhere in the same declaration, i.e.
    // { type l[b] = Either[Int, b]  }#l 
    // where 'l' can now be used to refer to an Either where the first type param has already
    // been supplied (currying, or partial application, for types)
    type λ[A] = Coproduct[F, G, A]
  }

  implicit class EnrichNT[F[_], H[_]](f: F ~> H) {
    // this operator combines f (a transform from F to H) with the argument 'g' (a transform
    // from G to H) to give us a new transform that goes from the the disjunction of F and G
    // to H (i.e. will transform an argument that's either an F or a G into a H)
    def :+:[G[_]](g: G ~> H): (G :+: F)#λ ~> H = new ((G :+: F)#λ ~> H) {
      // the argument to apply this transform to is the lambda type member of :+:, i.e. a 
      // coproduct of F and G, parameterized with A
      def apply[A](fa: (G :+: F)#λ[A]) = {
        // What is the type of fa.run? I think it's going to be either an F[A] or a G[A]
        // using scalaz disjunct: \/
        // so \/[F[A], G[A]], or F[A] \/ G[A]
        val r: G[A] \/ F[A] = fa.run
        // then folding that disjunction will apply either g or f (i.e. the transform from
        // F to H, or the transform from G to H) to whichever of G or F it was
        r.fold(g, f)
      }
    }
  }
}

