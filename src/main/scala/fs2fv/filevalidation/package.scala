package fs2fv

import scalaz.concurrent.Task

package object filevalidation {

  type PersistRowFailures = Vector[RowFailure] => Task[Vector[Int]]
  type ErrorThreshold = (Int, Int) => Boolean
  type FileValidator = (String, Int) => Task[(Int, Int)]

}
