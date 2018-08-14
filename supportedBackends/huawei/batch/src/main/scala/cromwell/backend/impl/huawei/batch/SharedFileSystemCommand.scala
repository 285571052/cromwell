package cromwell.backend.impl.huawei.batch

//hy:local
class SharedFileSystemCommand(val argv: Seq[Any]) {
  override def toString = argv.mkString("\"", "\" \"", "\"")
}

object SharedFileSystemCommand {
  def apply(program: String, programArgs: Any*) = new SharedFileSystemCommand(program +: programArgs)

  def apply(argv: Seq[Any]) = new SharedFileSystemCommand(argv)
}
