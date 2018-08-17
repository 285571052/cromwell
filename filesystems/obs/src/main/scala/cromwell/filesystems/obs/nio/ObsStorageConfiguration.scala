package cromwell.filesystems.obs.nio

import com.obs.services.ObsClient

object ObsStorageConfiguration {
  val ENDPOINT_KEY = "endpoint"
  val ACCESS_KEY = "access-key"
  val SECRET_Key = "secret-Key"

  import scala.collection.immutable.Map

  def parseMap(map: Map[String, Any]): ObsStorageConfiguration = {
    val endpoint = map.get(ENDPOINT_KEY) match {
      case Some(endpoint: String) if !endpoint.isEmpty => endpoint
      case _ => throw new IllegalArgumentException(s"endpoint is mandatory and must be an unempty string")
    }

    val accessKey = map.get(ACCESS_KEY) match {
      case Some(key: String) if !key.isEmpty => key
      case _ => throw new IllegalArgumentException(s"access-key is mandatory and must be an unempty string")
    }

    val secretKey = map.get(SECRET_Key) match {
      case Some(key: String) if !key.isEmpty => key
      case _ => throw new IllegalArgumentException(s"secret-key is mandatory and must be an unempty string")
    }

    new ObsStorageConfiguration(endpoint, accessKey, secretKey)
  }

  def getClient(map: Map[String, String]): ObsClient = {
    parseMap(map).newObsClient()
  }

  def getClient(endpoint: String,
                accessKey: String,
                secretKey: String): ObsClient = {
    ObsStorageConfiguration(endpoint, accessKey, secretKey).newObsClient()
  }

}

final case class ObsStorageConfiguration(endpoint: String,
                                         accessKey: String,
                                         secretKey: String,
                                        ) {

  import ObsStorageConfiguration._

  def toMap: Map[String, String] = {
    val ret = Map(ENDPOINT_KEY -> endpoint, ACCESS_KEY -> accessKey, SECRET_Key -> secretKey)
    ret
  }

  def newObsClient() = {
    new ObsClient(accessKey, secretKey, endpoint)
  }
}
