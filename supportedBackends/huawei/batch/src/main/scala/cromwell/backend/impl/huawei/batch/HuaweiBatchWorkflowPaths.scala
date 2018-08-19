package cromwell.backend.impl.huawei.batch

import com.typesafe.config.Config
import cromwell.backend.{BackendJobDescriptorKey, BackendWorkflowDescriptor}
import cromwell.backend.io.WorkflowPaths
import cromwell.core.path.PathBuilder

case class HuaweiBatchWorkflowPaths(override val workflowDescriptor: BackendWorkflowDescriptor,
                                    override val config: Config,
                                    override val pathBuilders: List[PathBuilder] = WorkflowPaths.DefaultPathBuilders) extends WorkflowPaths {


  override def toJobPaths(workflowPaths: WorkflowPaths, jobKey: BackendJobDescriptorKey): HuaweiBatchJobPaths = {
    new HuaweiBatchJobPaths(workflowPaths.asInstanceOf[HuaweiBatchWorkflowPaths], jobKey)
  }

  override protected def withDescriptor(workflowDescriptor: BackendWorkflowDescriptor): WorkflowPaths = this.copy(workflowDescriptor = workflowDescriptor)

  private[batch] def getWorkflowInputMounts: HuaweiBatchInputMount = {
    val src = workflowRoot
    val dest = HuaweiBatchJobPaths.HuaweiBatchTempInputDirectory.resolve(src.pathWithoutScheme)
    HuaweiBatchInputMount(src, dest, true)
  }
}
