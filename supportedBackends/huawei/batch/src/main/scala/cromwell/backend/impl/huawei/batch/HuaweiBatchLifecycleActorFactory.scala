package cromwell.backend.impl.huawei.batch

import cromwell.backend.{BackendConfigurationDescriptor}
import cromwell.backend.standard._

final case class HuaweiBatchLifecycleActorFactory(val name: String, val configurationDescriptor: BackendConfigurationDescriptor)
  extends StandardLifecycleActorFactory {
  override lazy val asyncExecutionActorClass: Class[_ <: StandardAsyncExecutionActor] = classOf[HuaweiBatchBackendJobExecutionActor]

  override def jobIdKey: String = HuaweiBatchBackendJobExecutionActor.JobIdKey
}
