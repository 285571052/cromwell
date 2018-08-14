package cromwell.backend.impl.huawei.batch

import akka.actor.Actor
import cromwell.backend.standard.StandardCachingActorHelper
import cromwell.core.logging.JobLogging

trait HuaweiBatchJobCachingActorHelper extends StandardCachingActorHelper {
  this: Actor with JobLogging =>
  lazy val initializationData: HuaweiBatchBackendInitializationData = {
    backendInitializationDataAs[HuaweiBatchBackendInitializationData]
  }

  lazy val huaweiBatchRegion = initializationData.huaweiBatchConfiguration.huaweiBatchRegion.getOrElse(throw new RuntimeException("no bcs client available"))

}
