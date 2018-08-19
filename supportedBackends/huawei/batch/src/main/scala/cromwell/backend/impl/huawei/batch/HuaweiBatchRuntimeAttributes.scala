package cromwell.backend.impl.huawei.batch

import common.validation.ErrorOr.ErrorOr
import wom.types.{WomArrayType, WomStringType, WomType}
import wom.values.{WomArray, WomString, WomValue}
import cats.data.Validated._
import cats.syntax.apply._
import cats.syntax.validated._
import com.typesafe.config.Config
import cromwell.backend.standard.StandardValidatedRuntimeAttributesBuilder
import cromwell.backend.validation._
import scala.util.{Failure, Success, Try}

trait OptionalWithDefault[A] {
  this: RuntimeAttributesValidation[A] =>
  protected val config: Option[Config]

  override protected def staticDefaultOption: Option[WomValue] = {
    Try(this.configDefaultWomValue(config)) match {
      case Success(value: Option[WomValue]) => value
      case Failure(_) => None
    }
  }
}

final case class HuaweiBatchRuntimeAttributes(
                                               timeout: Option[Int],
                                               tag: Option[String],
                                               mounts: Option[Seq[HuaweiBatchMount]]
                                             )

object HuaweiBatchRuntimeAttributes {
  val MountsKey = "mounts"
  val TimeoutKey = "timeout"
  val TagKey = "tag"

  private def timeoutValidation(runtimeConfig: Option[Config]): OptionalRuntimeAttributesValidation[Int] = TimeoutValidation.optionalWithDefault(runtimeConfig)

  private def tagValidation(runtimeConfig: Option[Config]): OptionalRuntimeAttributesValidation[String] = TagValidation.optionalWithDefault(runtimeConfig)

  private def mountsValidation(runtimeConfig: Option[Config]): OptionalRuntimeAttributesValidation[Seq[HuaweiBatchMount]] = MountsValidation.optionalWithDefault(runtimeConfig)

  def apply(validatedRuntimeAttributes: ValidatedRuntimeAttributes, backendRuntimeConfig: Option[Config]): HuaweiBatchRuntimeAttributes = {
    val timeout: Option[Int] = RuntimeAttributesValidation.extractOption(timeoutValidation(backendRuntimeConfig).key, validatedRuntimeAttributes)
    val tag: Option[String] = RuntimeAttributesValidation.extractOption(tagValidation(backendRuntimeConfig).key, validatedRuntimeAttributes)
    val mounts: Option[Seq[HuaweiBatchMount]] = RuntimeAttributesValidation.extractOption(mountsValidation(backendRuntimeConfig).key, validatedRuntimeAttributes)
    new HuaweiBatchRuntimeAttributes(
      timeout,
      tag,
      mounts
    )
  }

  def runtimeAttributesBuilder(backendRuntimeConfig: Option[Config]): StandardValidatedRuntimeAttributesBuilder = {
    val defaults = StandardValidatedRuntimeAttributesBuilder.default(backendRuntimeConfig).withValidation(
      timeoutValidation(backendRuntimeConfig),
      tagValidation(backendRuntimeConfig),
      mountsValidation(backendRuntimeConfig)
    )
    defaults
  }
}

object TimeoutValidation {
  def optionalWithDefault(config: Option[Config]): OptionalRuntimeAttributesValidation[Int] = new TimeoutValidation(config).optional
}

class TimeoutValidation(override val config: Option[Config]) extends IntRuntimeAttributesValidation(HuaweiBatchRuntimeAttributes.TimeoutKey) with OptionalWithDefault[Int]


object TagValidation {
  def optionalWithDefault(config: Option[Config]): OptionalRuntimeAttributesValidation[String] = new TagValidation(config).optional
}

class TagValidation(override val config: Option[Config]) extends StringRuntimeAttributesValidation("tag") with OptionalWithDefault[String]

object MountsValidation {
  def optionalWithDefault(config: Option[Config]): OptionalRuntimeAttributesValidation[Seq[HuaweiBatchMount]] = new MountsValidation(config).optional
}

class MountsValidation(override val config: Option[Config]) extends RuntimeAttributesValidation[Seq[HuaweiBatchMount]] with OptionalWithDefault[Seq[HuaweiBatchMount]] {
  override def key: String = HuaweiBatchRuntimeAttributes.MountsKey

  override def coercion: Traversable[WomType] = Set(WomStringType, WomArrayType(WomStringType))

  override protected def validateValue: PartialFunction[WomValue, ErrorOr[Seq[HuaweiBatchMount]]] = {
    case WomString(value) => validateMounts(value.split(",\\s*").toSeq)
    case WomArray(wdlType, values) if wdlType.memberType == WomStringType =>
      validateMounts(values.map(_.valueString))
  }

  private def validateMounts(mounts: Seq[String]): ErrorOr[Seq[HuaweiBatchMount]] = {
    val mountNels: Seq[ErrorOr[HuaweiBatchMount]] = mounts filter { s => !s.trim().isEmpty } map validateMounts
    val sequenced: ErrorOr[Seq[HuaweiBatchMount]] = sequenceNels(mountNels)
    sequenced
  }

  private def validateMounts(mount: String): ErrorOr[HuaweiBatchMount] = {
    HuaweiBatchMount.parse(mount) match {
      case scala.util.Success(mnt) => mnt.validNel
      case scala.util.Failure(ex) => ex.getMessage.invalidNel
    }
  }

  private def sequenceNels(nels: Seq[ErrorOr[HuaweiBatchMount]]): ErrorOr[Seq[HuaweiBatchMount]] = {
    val emptyMountNel: ErrorOr[Vector[HuaweiBatchMount]] = Vector.empty[HuaweiBatchMount].validNel
    val mountsNel: ErrorOr[Vector[HuaweiBatchMount]] = nels.foldLeft(emptyMountNel) {
      (acc, v) => (acc, v) mapN { (a, v) => a :+ v }
    }
    mountsNel
  }

  override protected def missingValueMessage: String =
    s"Expecting $key runtime attribute to be a comma separated String or Array[String]"
}