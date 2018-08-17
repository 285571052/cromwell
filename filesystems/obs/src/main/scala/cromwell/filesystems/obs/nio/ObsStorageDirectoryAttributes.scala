package cromwell.filesystems.obs.nio

import java.nio.file.attribute.FileTime

final case class ObsStorageDirectoryAttributes(path: ObsStoragePath) extends ObsStorageFileAttributes {
  override def creationTime(): FileTime = FileTime.fromMillis(0)

  override def lastAccessTime(): FileTime = FileTime.fromMillis(0)

  override def lastModifiedTime(): FileTime = creationTime()

  override def isRegularFile: Boolean = false

  override def isDirectory: Boolean = true

  override def isSymbolicLink: Boolean = false

  override def isOther: Boolean = false

  override def size(): Long = 0

  override def fileKey(): AnyRef = path.pathAsString

  override def expires: FileTime = FileTime.fromMillis(0)

  override def contentEncoding: Option[String] = None

  override def etag: Option[String] = None

}
