package cromwell.filesystems.obs.nio

import java.nio.file._
import java.nio.file.attribute.UserPrincipalLookupService
import java.{lang, util}
import java.util.Objects
import scala.collection.JavaConverters._


object ObsStorageFileSystem {
  val SEPARATOR: String = "/"
  val URI_SCHEMA: String = "obs"
  val OBS_VIEW = "obs"
  val BASIC_VIEW = "basic"

  def apply(provider: ObsStorageFileSystemProvider, bucket: String, obsStorageConfiguration: ObsStorageConfiguration): ObsStorageFileSystem = {
    val res = new ObsStorageFileSystem(bucket, obsStorageConfiguration)
    res.internalProvider = provider
    res
  }
}

case class ObsStorageFileSystem(bucket: String, config: ObsStorageConfiguration) extends FileSystem {

  var internalProvider: ObsStorageFileSystemProvider = ObsStorageFileSystemProvider(config)

  override def provider: ObsStorageFileSystemProvider = internalProvider

  override def getPath(first: String, more: String*): ObsStoragePath = ObsStoragePath.getPath(this, first, more: _*)

  override def close(): Unit = {
    // do nothing currently.
  }

  override def isOpen: Boolean = {
    true
  }

  override def isReadOnly: Boolean = {
    false
  }

  override def getSeparator: String = {
    ObsStorageFileSystem.SEPARATOR
  }

  override def getRootDirectories: lang.Iterable[Path] = {
    Set[Path](ObsStoragePath.getPath(this, UnixPath.ROOT_PATH)).asJava
  }

  override def getFileStores: lang.Iterable[FileStore] = {
    Set.empty[FileStore].asJava
  }

  override def getPathMatcher(syntaxAndPattern: String): PathMatcher = {
    FileSystems.getDefault.getPathMatcher(syntaxAndPattern)
  }

  override def getUserPrincipalLookupService: UserPrincipalLookupService = {
    throw new UnsupportedOperationException()
  }

  override def newWatchService(): WatchService = {
    throw new UnsupportedOperationException()
  }

  override def supportedFileAttributeViews(): util.Set[String] = {
    Set(ObsStorageFileSystem.OBS_VIEW, ObsStorageFileSystem.BASIC_VIEW).asJava
  }

  override def equals(obj: scala.Any): Boolean = {
    this == obj ||
      obj.isInstanceOf[ObsStorageFileSystem] &&
        obj.asInstanceOf[ObsStorageFileSystem].config.equals(config) &&
        obj.asInstanceOf[ObsStorageFileSystem].bucket.equals(bucket)
  }

  override def hashCode(): Int = Objects.hash(bucket)

  override def toString: String = ObsStorageFileSystem.URI_SCHEMA + "://" + bucket
}

