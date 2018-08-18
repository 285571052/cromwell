package cromwell.backend.impl.huawei.batch

import cromwell.core.ExecutionEvent
import scala.util.{Failure, Success, Try}
import com.huawei.batch.model.job.JobState

sealed trait RunStatus {

  import RunStatus._

  val jobId: String
  val status: String

  def isTerminated: Boolean

  def isRunningOrComplete = this match {
    case _: RUNNING | _: TerminalRunStatus => true
    case _ => false
  }

  override def toString = status
}

object RunStatus {

  final case class PENDING(override val jobId: String) extends RunStatus {
    override val status = "PENDING"

    override def isTerminated: Boolean = false
  }

  final case class RUNNING(override val jobId: String) extends RunStatus {
    override val status = "RUNNING"

    override def isTerminated: Boolean = false
  }

  final case class DELETING(override val jobId: String) extends RunStatus {
    override val status = "DELETING"

    override def isTerminated: Boolean = false
  }

  final case class TERMINATING(override val jobId: String) extends RunStatus {
    override val status = "TERMINATING"

    override def isTerminated: Boolean = false
  }

  final case class REACTIVATING(override val jobId: String) extends RunStatus {
    override val status = "REACTIVATING"

    override def isTerminated: Boolean = false
  }

  final case class ENDING(override val jobId: String) extends RunStatus {
    override val status = "ENDING"

    override def isTerminated: Boolean = false
  }


  sealed trait TerminalRunStatus extends RunStatus {
    def eventList: Seq[ExecutionEvent]

    override def isTerminated: Boolean = true
  }

  sealed trait UnsuccessfulRunStatus extends TerminalRunStatus {
    val errorMessage: Option[String]
    lazy val prettyPrintedError: String = errorMessage map { e => s" Message: $e" } getOrElse ""
  }

  final case class SUCCEEDED(override val jobId: String, eventList: Seq[ExecutionEvent]) extends TerminalRunStatus {
    override val status = "SUCCEEDED"
  }

  final case class FAILED(override val jobId: String,
                          errorMessage: Option[String],
                          eventList: Seq[ExecutionEvent]) extends UnsuccessfulRunStatus {
    override val status = "FAILED"
  }

  object UnsuccessfulRunStatus {
    def apply(jobId: String, status: JobState, errorMessage: Option[String], eventList: Seq[ExecutionEvent]): UnsuccessfulRunStatus = {
//      if (status == JobState.FAILED) {
        FAILED(jobId, errorMessage, eventList)
//      }
    }
  }

}

object RunStatusFactory {
  def getStatus(jobId: String, status: JobState, errorMessage: Option[String] = None, eventList: Option[Seq[ExecutionEvent]] = None): Try[RunStatus] = {
    import RunStatus._
    status match {
      case JobState.PENDING => Success(PENDING(jobId))
      case JobState.RUNNING => Success(RUNNING(jobId))
      case JobState.DELETING => Success(DELETING(jobId))
      case JobState.SUCCEEDED => Success(SUCCEEDED(jobId, eventList.getOrElse(Seq.empty)))
      case JobState.TERMINATING => Success(TERMINATING(jobId))
      case JobState.REACTIVATING => Success(REACTIVATING(jobId))
      case JobState.ENDING => Success(ENDING(jobId))
      case JobState.FAILED => Success(FAILED(jobId, errorMessage, eventList.getOrElse(Seq.empty)))
      case _ => Failure(new RuntimeException(s"job {$jobId} turns to an invalid batch job status: {$status}"))
    }
  }
}