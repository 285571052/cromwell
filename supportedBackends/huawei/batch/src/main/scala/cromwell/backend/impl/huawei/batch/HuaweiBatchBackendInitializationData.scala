package cromwell.backend.impl.huawei.batch

import cromwell.backend.io.WorkflowPaths
import cromwell.backend.standard.{StandardExpressionFunctions, StandardInitializationData, StandardValidatedRuntimeAttributesBuilder}

final case class HuaweiBatchBackendInitializationData
(
  override val workflowPaths: WorkflowPaths,
  override val runtimeAttributesBuilder: StandardValidatedRuntimeAttributesBuilder,
  huaweiBatchConfiguration: HuaweiBatchConfiguration
) extends StandardInitializationData(workflowPaths, runtimeAttributesBuilder, classOf[StandardExpressionFunctions])