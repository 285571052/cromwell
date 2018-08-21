package cromwell.backend.impl.huawei.batch

import akka.actor.ActorRef
import cromwell.backend.{BackendConfigurationDescriptor, BackendWorkflowDescriptor, BackendInitializationData}
import cromwell.backend.standard._
import cromwell.core.path.PathBuilder
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

  override lazy val pathBuilders: Future[List[PathBuilder]] =
    standardParams.configurationDescriptor.pathBuildersWithDefault(workflowDescriptor.workflowOptions)

  override lazy val workflowPaths: Future[HuaweiBatchWorkflowPaths] = pathBuilders map {
    HuaweiBatchWorkflowPaths(workflowDescriptor, huaweiBatchConfiguration.configurationDescriptor.backendConfig, _)
  }

  override lazy val runtimeAttributesBuilder: StandardValidatedRuntimeAttributesBuilder =
    HuaweiBatchRuntimeAttributes.runtimeAttributesBuilder(huaweiBatchConfiguration.runtimeConfig)

  override def beforeAll(): Future[Option[BackendInitializationData]] = {
    pathBuilders map { builders => HuaweiBatchMount.pathBuilders = builders }

    for {
      paths <- workflowPaths
      builders <- pathBuilders
    } yield Option(HuaweiBatchBackendInitializationData(paths, runtimeAttributesBuilder, huaweiBatchConfiguration, builders))
  }
}
