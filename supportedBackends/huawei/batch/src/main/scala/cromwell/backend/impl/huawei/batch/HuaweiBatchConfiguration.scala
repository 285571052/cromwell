package cromwell.backend.impl.huawei.batch

import cromwell.backend.BackendConfigurationDescriptor
import net.ceedubs.ficus.Ficus._

class HuaweiBatchConfiguration(val configurationDescriptor: BackendConfigurationDescriptor) {
  val runtimeConfig = configurationDescriptor.backendRuntimeConfig
  val huaweiBatchRegion: Option[String] = configurationDescriptor.backendConfig.as[Option[String]]("region")
  //val user = configurationDescriptor.backendConfig.as[Option[String]]("user").getOrElse("")
}
