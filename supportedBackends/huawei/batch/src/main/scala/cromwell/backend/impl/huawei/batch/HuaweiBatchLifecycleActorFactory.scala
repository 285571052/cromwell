package cromwell.backend.impl.huawei.batch

import akka.actor.ActorRef
import cromwell.backend.{BackendConfigurationDescriptor, BackendWorkflowDescriptor}
import cromwell.backend.standard._
import wom.graph.CommandCallNode

final case class HuaweiBatchLifecycleActorFactory(val name: String, val configurationDescriptor: BackendConfigurationDescriptor)
  extends StandardLifecycleActorFactory {
  override lazy val initializationActorClass: Class[_ <: StandardInitializationActor] = classOf[HuaweiBatchInitializationActor]
  override lazy val asyncExecutionActorClass: Class[_ <: StandardAsyncExecutionActor] = classOf[HuaweiBatchBackendJobExecutionActor]

  override def jobIdKey: String = HuaweiBatchBackendJobExecutionActor.JobIdKey

  val huaweiBatchConfiguration = new HuaweiBatchConfiguration(configurationDescriptor)

  override def workflowInitializationActorParams(workflowDescriptor: BackendWorkflowDescriptor, ioActor: ActorRef, calls: Set[CommandCallNode], serviceRegistryActor: ActorRef, restarting: Boolean): StandardInitializationActorParams = {
    HuaweiBatchInitializationActorParams(workflowDescriptor, calls, huaweiBatchConfiguration, serviceRegistryActor)
  }
}
