package cromwell.backend.impl.huawei.batch

import cromwell.backend.io.WorkflowPaths
import cromwell.backend.standard.{StandardInitializationData, StandardValidatedRuntimeAttributesBuilder}
import cromwell.core.path.PathBuilder

final case class HuaweiBatchBackendInitializationData
(
  override val workflowPaths: WorkflowPaths,
  override val runtimeAttributesBuilder: StandardValidatedRuntimeAttributesBuilder,
  huaweiBatchConfiguration: HuaweiBatchConfiguration,
  pathBuilders: List[PathBuilder]
) extends StandardInitializationData(workflowPaths, runtimeAttributesBuilder, classOf[HuaweiBatchExpressFunctions])