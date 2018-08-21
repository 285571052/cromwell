package cromwell.backend.impl.huawei.batch

import common.validation.ErrorOr.ErrorOr
import cromwell.backend.BackendJobDescriptor
import cromwell.backend.standard.{StandardExpressionFunctions, StandardExpressionFunctionsParams}
import cromwell.filesystems.obs.ObsPathBuilder
import cromwell.filesystems.obs.ObsPathBuilder.{InvalidObsPath, PossiblyValidRelativeObsPath, ValidFullObsPath}
import wom.graph.CommandCallNode
import wom.values.WomGlobFile

import scala.concurrent.Future

final case class HuaweiBatchExpressFunctions(override val standardParams: StandardExpressionFunctionsParams)
  extends StandardExpressionFunctions(standardParams) {

  override def preMapping(str: String) = {
    ObsPathBuilder.validateObsPath(str) match {
      case _: ValidFullObsPath => str
      case PossiblyValidRelativeObsPath => callContext.root.resolve(str.stripPrefix("/")).pathAsString
      case invalid: InvalidObsPath => throw new IllegalArgumentException(invalid.errorMessage)
    }
  }

  override def glob(pattern: String): Future[Seq[String]] = {
    if (pattern == "cwl.output.json") {
      Future.successful(Nil)
    } else {
      super.glob(pattern)
    }
  }

  override def findGlobOutputs(call: CommandCallNode, jobDescriptor: BackendJobDescriptor): ErrorOr[List[WomGlobFile]] = {
    val base = super.findGlobOutputs(call, jobDescriptor)
    base.map(_.filterNot(_.value == "cwl.output.json"))
  }
}
