package cromwell.backend.impl.huawei.batch

import cromwell.core.ExecutionEvent
import scala.util.{Failure, Success, Try}

//TODO: add more status
sealed trait RunStatus {

  import RunStatus._

  val jobId: String
  val status: String

  def isRunningOrComplete = this match {
    case _: Running | _: TerminalRunStatus => true
    case _ => false
  }

  override def toString = status
}

object RunStatus {

  final case class Running(override val jobId: String) extends RunStatus {
    override val status = "Running"
  }

  sealed trait TerminalRunStatus extends RunStatus {
    def eventList: Seq[ExecutionEvent]
  }

  final case class Finished(override val jobId: String, eventList: Seq[ExecutionEvent]) extends TerminalRunStatus {
    override val status = "Finished"
  }

}

object RunStatusFactory {
  def getStatus(jobId: String, status: String, eventList: Option[Seq[ExecutionEvent]] = None): Try[RunStatus] = {
    import RunStatus._
    status match {
      case "Running" => Success(Running(jobId))
      case "Finished" => Success(Finished(jobId, eventList.getOrElse(Seq.empty)))
      case _ => Failure(new RuntimeException(s"job {$jobId} turns to an invalid batchcompue status: {$status}"))
    }
  }
}