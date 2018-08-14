package cromwell.backend.impl.huawei.batch

import akka.actor.ActorRef
import cromwell.backend.{BackendConfigurationDescriptor, BackendWorkflowDescriptor}
import cromwell.backend.standard._
import wom.graph.CommandCallNode

import scala.concurrent.Future


final case class HuaweiBatchInitializationActorParams
(
  workflowDescriptor: BackendWorkflowDescriptor,
  calls: Set[CommandCallNode],
  huaweiBatchConfiguration: HuaweiBatchConfiguration,
  serviceRegistryActor: ActorRef
) extends StandardInitializationActorParams {
  override val configurationDescriptor: BackendConfigurationDescriptor = huaweiBatchConfiguration.configurationDescriptor
}

final class HuaweiBatchInitializationActor(params: HuaweiBatchInitializationActorParams)
  extends StandardInitializationActor(params) {
  private val huaweiBatchConfiguration = params.huaweiBatchConfiguration
  override lazy val initializationData: Future[HuaweiBatchBackendInitializationData] =
    workflowPaths map {
      new HuaweiBatchBackendInitializationData(_, runtimeAttributesBuilder, huaweiBatchConfiguration)
    }
}
