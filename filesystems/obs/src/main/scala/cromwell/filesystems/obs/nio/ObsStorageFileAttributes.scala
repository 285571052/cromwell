package cromwell.filesystems.obs.nio

import java.nio.file.attribute.{BasicFileAttributes, FileTime}

trait ObsStorageFileAttributes extends BasicFileAttributes {
  def contentEncoding: Option[String]

  def expires: FileTime

  def etag: Option[String]
}
