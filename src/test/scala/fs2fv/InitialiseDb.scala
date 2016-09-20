package fs2fv

import doobie.imports._
import doobie.util.transactor.Transactor

import scalaz.concurrent.Task

trait InitialiseDb {

  def dbName: String

  def initialiseDb(): Transactor[Task] = {
    val xa = DriverManagerTransactor[Task](
      "org.h2.Driver", s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1", "sa", ""
    )
    val ddl = sql"runscript from 'src/main/resources/fs2fv.ddl.sql'".update
    xa.trans(ddl.run).unsafePerformSync
    xa
  }
}
