package cromwell.backend.impl.huawei.batch

import akka.actor.Actor
import cromwell.backend.standard.StandardCachingActorHelper
import cromwell.core.logging.JobLogging
import cromwell.core.path.Path

trait HuaweiBatchJobCachingActorHelper extends StandardCachingActorHelper {
  this: Actor with JobLogging =>
  lazy val initializationData: HuaweiBatchBackendInitializationData = {
    backendInitializationDataAs[HuaweiBatchBackendInitializationData]
  }
  lazy val huaweiBatchClient = initializationData.huaweiBatchConfiguration.huaweiBatchClient.getOrElse(throw new RuntimeException("no huawei batch client available"))

  lazy val huaweiBatchWorkflowPaths: HuaweiBatchWorkflowPaths = workflowPaths.asInstanceOf[HuaweiBatchWorkflowPaths]

  lazy val huaweiBatchJobPaths: HuaweiBatchJobPaths = jobPaths.asInstanceOf[HuaweiBatchJobPaths]

  lazy val huaweiBatchConfiguration: HuaweiBatchConfiguration = initializationData.huaweiBatchConfiguration

  lazy val runtimeAttributes = HuaweiBatchRuntimeAttributes(validatedRuntimeAttributes, huaweiBatchConfiguration.runtimeConfig)

  lazy val callRootPath: Path = huaweiBatchJobPaths.callExecutionRoot

  lazy val returnCodeFilename: String = huaweiBatchJobPaths.returnCodeFilename
  lazy val returnCodeGcsPath: Path = huaweiBatchJobPaths.returnCode
  lazy val standardPaths = huaweiBatchJobPaths.standardPaths
  lazy val huaweiBatchStdoutFile: Path = standardPaths.output
  lazy val huaweiBatchStderrFile: Path = standardPaths.error
}
