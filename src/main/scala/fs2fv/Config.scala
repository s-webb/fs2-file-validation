package fs2fv

import java.util.regex.Pattern

case class Config(output: List[Output])

case class Output(name: String, inputs: List[Input])

case class Input(name: String, columns: List[Column])

case class Column(name: String, pattern: String) {
  val re = Pattern.compile(pattern)
}
