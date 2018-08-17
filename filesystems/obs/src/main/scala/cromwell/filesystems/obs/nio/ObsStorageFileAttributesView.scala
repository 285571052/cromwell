package cromwell.filesystems.obs.nio

import java.nio.file.NoSuchFileException
import java.nio.file.attribute.{BasicFileAttributeView, FileTime}

import scala.util.Try
import com.obs.services.ObsClient
import com.obs.services.model.ObsObject

import scala.util.control.Breaks.break
import scala.collection.JavaConverters._

final case class ObsStorageFileAttributesView(obsClient: ObsClient, path: ObsStoragePath) extends BasicFileAttributeView {
  override def name(): String = ObsStorageFileSystem.URI_SCHEMA

  override def readAttributes(): ObsStorageFileAttributes = {
    val obsPath = ObsStoragePath.checkPath(path)

    if (obsPath.seemsLikeDirectory) {
      return ObsStorageDirectoryAttributes(path)
    }

    var exist = false
    val obsObjectList: java.util.List[ObsObject] = obsClient.listObjects(obsPath.bucket).getObjects()
    for (obsObject <- obsObjectList.asScala) {
      if (obsObject.getObjectKey() == obsPath.key) {
        exist = true
        break
      }
    }
    if (!exist) {
      throw new NoSuchFileException(path.toString)
    }


    val objectMeta = ObsStorageRetry.fromTry(
      () => Try {
        obsClient.getObjectMetadata(path.bucket, path.key)
      }
    )

    ObsStorageObjectAttributes(objectMeta, path)
  }

  override def setTimes(lastModifiedTime: FileTime, lastAccessTime: FileTime, createTime: FileTime): Unit = throw new UnsupportedOperationException("OBS object is immutable")
}
