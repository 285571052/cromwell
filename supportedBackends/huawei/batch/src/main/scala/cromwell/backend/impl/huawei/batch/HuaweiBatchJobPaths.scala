package cromwell.backend.impl.huawei.batch

import cromwell.backend.BackendJobDescriptorKey
import cromwell.backend.io.JobPaths
import cromwell.core.path.{DefaultPathBuilder, Path}

object HuaweiBatchJobPaths {
  val HuaweiBatchLogPathKey = "huaweiBatchLog"
  val HuaweiBatchEnvExecKey = "exec"
  val HuaweiBatchEnvCwdKey = "cwd"
  val HuaweiBatchEnvStdoutKey = "stdout"
  val HuaweiBatchEnvStderrKey = "stderr"
  val HuaweiBatchCommandDirectory: Path = DefaultPathBuilder.get("/cromwell_root")
  val HuaweiBatchTempInputDirectory: Path = DefaultPathBuilder.get("/cromwell_inputs")
  val HuaweiBatchStdoutRedirectPath = "huaweiBatch-stdout"
  val HuaweiBatchStderrRedirectPath = "huaweiBatch-stderr"
}

final case class HuaweiBatchJobPaths(workflowPaths: HuaweiBatchWorkflowPaths, jobKey: BackendJobDescriptorKey) extends JobPaths {

  import HuaweiBatchJobPaths._

  val huaweiBatchStdoutPath = callRoot.resolve(HuaweiBatchStdoutRedirectPath)
  val huaweiBatchStderrPath = callRoot.resolve(HuaweiBatchStderrRedirectPath)
}
