package cromwell.filesystems.obs.nio

import com.obs.services.model.ObjectMetadata
import java.nio.file.attribute.FileTime

final case class ObsStorageObjectAttributes(objectMeta: ObjectMetadata, path: ObsStoragePath) extends ObsStorageFileAttributes {
  override def creationTime(): FileTime = {
    FileTime.fromMillis(objectMeta.getLastModified.getTime)
  }

  override def lastAccessTime(): FileTime = FileTime.fromMillis(0)

  override def lastModifiedTime(): FileTime = creationTime()

  override def isRegularFile: Boolean = true

  override def isDirectory: Boolean = false

  override def isSymbolicLink: Boolean = false

  override def isOther: Boolean = false

  override def size(): Long = objectMeta.getContentLength

  override def fileKey(): AnyRef = path.pathAsString

  override def expires: FileTime = FileTime.fromMillis(0)

  override def contentEncoding: Option[String] = Option(objectMeta.getContentEncoding)

  override def etag: Option[String] = Option(objectMeta.getEtag)
}
