package cromwell.backend.impl.huawei.batch

import cromwell.backend.io.JobPaths
import cromwell.core.ExecutionEvent

import scala.util.Try
import org.slf4j.LoggerFactory

//TODO: add batch client
final case class HuaweiBatchJob(jobPaths: JobPaths) {

  def submit(): Try[String] = Try {
    val logger = LoggerFactory.getLogger("HuaweiBatchJob")
    logger.warn("HuaweiBacthJob.submit()")
    val jobId = "job_id"
    jobId
  }

  def getStatus(jobId: String): Try[RunStatus] = {
    val status = "Finished"
    val eventList = Seq[ExecutionEvent]()
    RunStatusFactory.getStatus(jobId, status, Some(eventList))
  }
}
