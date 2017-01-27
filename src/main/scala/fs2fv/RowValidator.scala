package fs2fv

import fs2._
import fs2fv.ValidateAndMerge._

object RowValidator {

  def rowValidator[F[_]](inCfg: Input): Pipe[F, TokenizedLine, Either[RowFailure, TokenizedLine]] =
    _.map { case ln@(tokens, linenum) =>
      if (tokens.size == inCfg.columns.size) {
        // What other validations might the input config specify? It's just a regex, isn't it?
        val failures: Seq[String] = tokens.zipWithIndex.flatMap { case (t, n) =>
          val p = inCfg.columns(n).re
          if (p.matcher(t).matches) {
            None
          } else { Some(s"token ${n}, '${t}', does not match pattern ${p}") }
        }
        if (!failures.isEmpty) {
          Left((ln, failures))
        } else {
          Right(ln)
        }
      } else {
        Left((ln, Seq(s"expected ${inCfg.columns.size} tokens, got ${tokens.size}")))
      }
    }
}
