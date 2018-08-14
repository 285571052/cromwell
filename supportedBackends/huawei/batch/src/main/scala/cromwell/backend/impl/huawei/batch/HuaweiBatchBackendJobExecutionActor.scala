package cromwell.backend.impl.huawei.batch

import cromwell.backend.BackendJobLifecycleActor
import cromwell.core.retry.SimpleExponentialBackoff
import cromwell.backend.impl.huawei.batch.RunStatus.TerminalRunStatus

import scala.concurrent.duration._
import scala.concurrent.Future
import cromwell.backend.async.{ExecutionHandle, FailedNonRetryableExecutionHandle, PendingExecutionHandle}
import cromwell.backend.standard.{StandardAsyncExecutionActor, StandardAsyncExecutionActorParams, StandardAsyncJob}
import cromwell.core.path.Path

import scala.util.Try

object HuaweiBatchBackendJobExecutionActor {
  val JobIdKey = "__huawei_batch_job_id"
}

class HuaweiBatchBackendJobExecutionActor(override val standardParams: StandardAsyncExecutionActorParams) extends BackendJobLifecycleActor with StandardAsyncExecutionActor with HuaweiBatchJobCachingActorHelper {
  /** The type of the run info when a job is started. */
  override type StandardAsyncRunInfo = HuaweiBatchJob
  /** The type of the run status returned during each poll. */
  override type StandardAsyncRunStatus = RunStatus

  type HuaweiBatchPendingExecutionHandle = PendingExecutionHandle[StandardAsyncJob, HuaweiBatchJob, RunStatus]

  /**
    * Returns true when a job is complete, either successfully or unsuccessfully.
    *
    * @param runStatus The run status.
    * @return True if the job has completed.
    */
  override def isTerminal(runStatus: RunStatus): Boolean = {
    runStatus match {
      case _: TerminalRunStatus => true
      case _ => false
    }
  }

  override lazy val dockerImageUsed: Option[String] = None

  override lazy val pollBackOff = SimpleExponentialBackoff(1.second, 5.minutes, 1.1)

  override lazy val executeOrRecoverBackOff = SimpleExponentialBackoff(3.seconds, 30.seconds, 1.1)

  lazy val standardPaths = jobPaths.standardPaths //hy:local
  lazy val backgroundScript = jobPaths.script.plusExt("background")

  //hy:local
  def writeScriptContents(): Either[ExecutionHandle, Unit] = {
    writeScriptContentshtlp().flatMap(_ => writeBackgroundScriptContents())
  }

  def processArgs: SharedFileSystemCommand = {
    val script = jobPaths.script.pathAsString
    SharedFileSystemCommand("/bin/bash", script)
  }

  /**
    * Run the command via bash in the background, and echo the PID.
    */
  private def writeBackgroundScriptContents(): Either[ExecutionHandle, Unit] = {
    val backgroundCommand = redirectOutputs(processArgs.argv.mkString("'", "' '", "'"))
    // $! contains the previous background command's process id (PID)
    backgroundScript.write(
      s"""|#!/bin/bash
          |BACKGROUND_COMMAND &
          |echo $$!
          |""".stripMargin.replace("BACKGROUND_COMMAND", backgroundCommand))
    Right(())
  } //hy:local

  def writeScriptContentshtlp(): Either[ExecutionHandle, Unit] =
    commandScriptContents.fold(
      errors => Left(FailedNonRetryableExecutionHandle(new RuntimeException("Unable to start job due to: " + errors.toList.mkString(", ")))),
      { script => jobPaths.script.write(script); Right(()) }) //hy:local

  def getJob(exitValue: Int, stdout: Path, stderr: Path): StandardAsyncJob = {
    val pid = stdout.contentAsString.stripLineEnd
    StandardAsyncJob(pid)
  } //hy:local

  def makeProcessRunner(): ProcessRunner = {
    val stdout = standardPaths.output.plusExt("background")
    val stderr = standardPaths.error.plusExt("background")
    val argv = Seq("/bin/bash", backgroundScript)
    new ProcessRunner(argv, stdout, stderr)
  } //hy:local

  override def execute(): ExecutionHandle = {
    jobPaths.callExecutionRoot.createDirectories()
    writeScriptContents().fold(
      identity,
      { _ =>
        val runner = makeProcessRunner()
        val exitValue = runner.run()
        if (exitValue != 0) {
          FailedNonRetryableExecutionHandle(new RuntimeException("Unable to start job. " +
            s"Check the stderr file for possible errors: ${runner.stderrPath}"))
        } else {
          val runningJob = getJob(exitValue, runner.stdoutPath, runner.stderrPath)
          PendingExecutionHandle(jobDescriptor, runningJob, None, None)
        }
      }
    )
  } //hy:local

  override def executeAsync(): Future[ExecutionHandle] = {
    Future.fromTry(Try(execute()))
    val huaweiBatchJob = new HuaweiBatchJob(jobPaths, huaweiBatchRegion)
    for {
      jobId <- Future.fromTry(huaweiBatchJob.submit())
    } yield PendingExecutionHandle(jobDescriptor, StandardAsyncJob(jobId), Option(huaweiBatchJob), previousStatus = None)
  }

  override def recoverAsync(jobId: StandardAsyncJob) = executeAsync()

  override def pollStatusAsync(handle: HuaweiBatchPendingExecutionHandle): Future[RunStatus] = {
    val jobId = handle.pendingJob.jobId
    val huaweiBatchJob: HuaweiBatchJob = handle.runInfo.getOrElse(throw new RuntimeException("empty run job info "))

    Future.fromTry(huaweiBatchJob.getStatus(jobId))
  }

}
