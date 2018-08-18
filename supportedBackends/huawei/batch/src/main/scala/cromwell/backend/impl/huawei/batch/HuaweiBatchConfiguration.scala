package cromwell.backend.impl.huawei.batch

import cromwell.backend.BackendConfigurationDescriptor
import net.ceedubs.ficus.Ficus._
import com.huawei.batch.client.{BatchApi, BatchService}
//import org.slf4j.LoggerFactory

object HuaweiBatchConfiguration {
  val ObsEndpointKey = "obsEndpoint"
  val ObsAccessKey = "obsAccessKey"
  val ObsSecretKey = "obsSecretKey"
}

class HuaweiBatchConfiguration(val configurationDescriptor: BackendConfigurationDescriptor) {
  val runtimeConfig = configurationDescriptor.backendRuntimeConfig

  val huaweiBatchRegion: Option[String] = configurationDescriptor.backendConfig.as[Option[String]]("region")

  val huaweiBatchUser: Option[String] = configurationDescriptor.backendConfig.as[Option[String]]("user")

  val huaweiBatchPass: Option[String] = configurationDescriptor.backendConfig.as[Option[String]]("pass")

  val huaweiBatchEndpoint: Option[String] = configurationDescriptor.backendConfig.as[Option[String]]("endpoint")


  val obsEndpoint = configurationDescriptor.backendConfig.as[Option[String]]("filesystems.obs.auth.endpoint").getOrElse("")
  val obsAccessKey = configurationDescriptor.backendConfig.as[Option[String]]("filesystems.obs.auth.access-key").getOrElse("")
  val obsSecretKey = configurationDescriptor.backendConfig.as[Option[String]]("filesystems.obs.auth.secret-key").getOrElse("")

  val huaweiBatchClient: Option[BatchApi] = {
    for {
      user <- huaweiBatchUser
      pass <- huaweiBatchPass
      region <- huaweiBatchRegion
      endpoint <- huaweiBatchEndpoint
    } yield new BatchService(
      user, pass, region, endpoint
    ).getBatchClient()
  }
}
