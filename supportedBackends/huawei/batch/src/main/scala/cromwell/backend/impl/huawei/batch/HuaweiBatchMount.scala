package cromwell.backend.impl.huawei.batch

import cats.data.Validated._
import cats.syntax.apply._
import cats.syntax.validated._
import common.exception.MessageAggregation
import common.validation.ErrorOr._
import cromwell.core.path.{DefaultPathBuilder, Path, PathBuilder, PathFactory}

import scala.util.Try
import scala.util.matching.Regex

object HuaweiBatchMount {
  var pathBuilders: List[PathBuilder] = List()

  val obsPrefix = """obs://[^\s]+"""
  val localPath = """/[^\s]+"""
  val writeSupport = """true|false"""

  val inputMountPattern: Regex = s"""($obsPrefix)\\s+($localPath)\\s+($writeSupport)""".r
  val outputMountPattern: Regex = s"""($localPath)\\s+($obsPrefix)\\s+($writeSupport)""".r

  def parse(s: String): Try[HuaweiBatchMount] = {
    val validation: ErrorOr[HuaweiBatchMount] = s match {
      case inputMountPattern(obs, local, writeSupport) =>
        (validateObs(obs), validateLocal(obs, local), validateBoolean(writeSupport)) mapN { (src, dest, ws) => new HuaweiBatchInputMount(src, dest, ws)}
      case outputMountPattern(local, obs, writeSupport) =>
        (validateLocal(obs, local), validateObs(obs), validateBoolean(writeSupport)) mapN { (src, dest, ws) => new HuaweiBatchOutputMount(src, dest, ws)}
      case _ => s"Mount strings should be of the format 'obs://my-bucket/inputs/ /home/inputs/ true' or '/home/outputs/ obs://my-bucket/outputs/ false'".invalidNel
    }

    Try(validation match {
      case Valid(mount) => mount
      case Invalid(nels) =>
        throw new UnsupportedOperationException with MessageAggregation {
          val exceptionContext = ""
          val errorMessages: List[String] = nels.toList
        }
    })
  }

  private def validateObs(value: String): ErrorOr[Path] = {
    PathFactory.buildPath(value, pathBuilders).validNel
  }
  private def validateLocal(obs: String, local: String): ErrorOr[Path] = {
    if (obs.endsWith("/") == local.endsWith("/")) {
      DefaultPathBuilder.get(local).validNel
    } else {
      "obs and local path type not match".invalidNel
    }
  }

  private def validateBoolean(value: String): ErrorOr[Boolean] = {
    try {
      value.toBoolean.validNel
    } catch {
      case _: IllegalArgumentException => s"$value not convertible to a Boolean".invalidNel
    }
  }

}

trait HuaweiBatchMount {
  var src: Path
  var dest: Path
  var writeSupport: Boolean
}

final case class HuaweiBatchInputMount(var src: Path, var dest: Path, var writeSupport: Boolean) extends HuaweiBatchMount
final case class HuaweiBatchOutputMount(var src: Path, var dest: Path, var writeSupport: Boolean) extends HuaweiBatchMount