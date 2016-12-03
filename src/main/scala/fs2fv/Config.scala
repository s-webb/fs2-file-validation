package fs2fv

case class Config(output: List[Output])

case class Output(name: String, inputs: List[Input])

case class Input(name: String, columns: List[Column])

case class Column(name: String, maxWidth: Int, required: Boolean)
