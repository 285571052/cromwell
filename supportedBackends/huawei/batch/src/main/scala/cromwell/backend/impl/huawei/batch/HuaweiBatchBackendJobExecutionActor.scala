package cromwell.backend.impl.huawei.batch

import cats.data.Validated.Valid
import cromwell.backend.BackendJobLifecycleActor
import cromwell.backend.impl.huawei.batch.RunStatus.{SUCCEEDED, TerminalRunStatus}
import cromwell.backend._
import cromwell.backend.async.{ExecutionHandle, FailedNonRetryableExecutionHandle, PendingExecutionHandle}
import cromwell.backend.standard.{StandardAsyncExecutionActor, StandardAsyncExecutionActorParams, StandardAsyncJob}
import cromwell.core.path.{DefaultPathBuilder, Path, PathFactory}
import cromwell.core.retry.SimpleExponentialBackoff
import cromwell.core.ExecutionEvent
import cromwell.filesystems.obs.ObsPath
import wom.callable.Callable.OutputDefinition
import wom.core.FullyQualifiedName
import wom.expression.NoIoFunctionSet
import wom.types.WomSingleFileType
import wom.values._
import com.huawei.batch.client.ApiException;
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Success, Try}

object HuaweiBatchBackendJobExecutionActor {
  val JobIdKey = "__huawei_batch_job_id"
}

class HuaweiBatchBackendJobExecutionActor(override val standardParams: StandardAsyncExecutionActorParams) extends BackendJobLifecycleActor with StandardAsyncExecutionActor with HuaweiBatchJobCachingActorHelper {

  override type StandardAsyncRunInfo = HuaweiBatchJob

  override type StandardAsyncRunStatus = RunStatus

  type HuaweiBatchPendingExecutionHandle = PendingExecutionHandle[StandardAsyncJob, HuaweiBatchJob, RunStatus]


  override lazy val dockerImageUsed: Option[String] = None

  override lazy val commandDirectory: Path = HuaweiBatchJobPaths.HuaweiBatchCommandDirectory.resolve(huaweiBatchJobPaths.callExecutionRoot.pathWithoutScheme)

  private[batch] lazy val userTag = runtimeAttributes.tag.getOrElse("cromwell")

  override lazy val pollBackOff = SimpleExponentialBackoff(1.second, 5.minutes, 1.1)

  override lazy val executeOrRecoverBackOff = SimpleExponentialBackoff(3.seconds, 30.seconds, 1.1)

  private[batch] lazy val jobName: String =
    List(userTag, jobDescriptor.workflowDescriptor.id.shortString, jobDescriptor.taskCall.identifier.localName.value)
      .mkString("-")
      .replaceAll("[^a-zA-Z0-9-]", "-")
      .toLowerCase()

  override lazy val jobTag: String = jobDescriptor.key.tag

  private lazy val huaweiBatchWorkflowInputMount: HuaweiBatchMount = huaweiBatchWorkflowPaths.getWorkflowInputMounts

  private lazy val userDefinedMounts: List[HuaweiBatchMount] = runtimeAttributes.mounts.toList.flatten :+ huaweiBatchWorkflowInputMount

  private var inputMounts: List[HuaweiBatchMount] = List.empty

  private[batch] def obsPathToMount(obsPath: ObsPath): HuaweiBatchInputMount = {
    val tmp = DefaultPathBuilder.get("/" + obsPath.pathWithoutScheme)
    val dir = tmp.getParent
    val local = HuaweiBatchJobPaths.HuaweiBatchTempInputDirectory.resolve(dir.pathAsString.md5SumShort).resolve(tmp.getFileName)
    val ret = HuaweiBatchInputMount(obsPath, local, writeSupport = false)
    if (!inputMounts.exists(mount => mount.src == obsPath && mount.dest == local)) {
      inputMounts :+= ret
    }
    ret
  }

  private[batch] def womFileToMount(file: WomFile): Option[HuaweiBatchInputMount] = file match {
    case path if userDefinedMounts exists (huaweiBatchMount => path.valueString.startsWith(huaweiBatchMount.src.pathAsString)) => None
    case path => PathFactory.buildPath(path.valueString, initializationData.pathBuilders) match {
      case obsPath: ObsPath => Some(obsPathToMount(obsPath))
      case _ => None
    }
  }

  private def huaweiBatchInputsFromWomFiles(prefix: String,
                                            remotePathArray: Seq[WomFile],
                                            jobDescriptor: BackendJobDescriptor): Iterable[HuaweiBatchInputMount] = {
    remotePathArray flatMap { remotePath =>
      womFileToMount(remotePath) match {
        case Some(mount) => Seq(mount)
        case None => Seq.empty
      }
    }
  }

  private[batch] def getInputFiles(jobDescriptor: BackendJobDescriptor): Map[FullyQualifiedName, Seq[WomFile]] = {
    val writeFunctionFiles = instantiatedCommand.createdFiles map { f => f.file.value.md5SumShort -> Seq(f.file) }

    val writeFunctionInputs = writeFunctionFiles map {
      case (name, files) => name -> files
    }

    // Collect all WomFiles from inputs to the call.
    val callInputFiles: Map[FullyQualifiedName, Seq[WomFile]] = jobDescriptor.fullyQualifiedInputs mapValues {
      _.collectAsSeq { case w: WomFile => w }
    }

    callInputFiles ++ writeFunctionInputs
  }

  private[batch] def generateHuaweiBatchInputs(jobDescriptor: BackendJobDescriptor): Unit = {
    val _ = getInputFiles(jobDescriptor) flatMap {
      case (name, files) => huaweiBatchInputsFromWomFiles(name, files, jobDescriptor)
    }
  }

  private def relativePath(path: String): Path = {
    val absolutePath = DefaultPathBuilder.get(path) match {
      case p if !p.isAbsolute => commandDirectory.resolve(p)
      case p => p
    }

    absolutePath
  }

  private[batch] lazy val callRawOutputFiles: List[WomFile] = {
    import cats.syntax.validated._
    def evaluateFiles(output: OutputDefinition): List[WomFile] = {
      Try(
        output.expression.evaluateFiles(jobDescriptor.localInputs, NoIoFunctionSet, output.womType).map(_.toList map {
          _.file
        })
      ).getOrElse(List.empty[WomFile].validNel)
        .getOrElse(List.empty)
    }

    // val womFileOutputs = call.task.findOutputFiles(jobDescriptor.fullyQualifiedInputs, PureStandardLibraryFunctions)

    jobDescriptor.taskCall.callable.outputs.flatMap(evaluateFiles)
  }

  private[batch] def isOutputObsFileString(s: String): Boolean = {
    callRawOutputFiles.exists({
      case file: WomSingleFile if file.value == s => true
      case _ => false
    })
  }

  private[batch] def relativeOutputPath(path: Path): Path = {
    if (isOutputObsFileString(path.pathAsString)) {
      huaweiBatchJobPaths.callRoot.resolve(path.pathAsString).normalize()
    } else {
      path
    }
  }

  private def generateHuaweiBatchSingleFileOutput(wdlFile: WomSingleFile): HuaweiBatchOutputMount = {
    val destination = getPath(wdlFile.valueString) match {
      case Success(obsPath: ObsPath) => obsPath
      case Success(path: Path) if !path.isAbsolute => relativeOutputPath(path)
      case _ => callRootPath.resolve(wdlFile.value.stripPrefix("/"))
    }

    val src = relativePath(wdlFile.valueString)

    HuaweiBatchOutputMount(src, destination, writeSupport = false)
  }

  private[batch] def generateHuaweiBatchOutputs(jobDescriptor: BackendJobDescriptor): Seq[HuaweiBatchMount] = {
    callRawOutputFiles.flatMap(_.flattenFiles).distinct flatMap { womFile =>
      womFile match {
        case singleFile: WomSingleFile => List(generateHuaweiBatchSingleFileOutput(singleFile))
        case _: WomGlobFile => throw new RuntimeException(s"glob output not supported currently")
        case _: WomUnlistedDirectory => throw new RuntimeException(s"directory output not supported currently")
      }
    }
  }

  private[batch] def getObsFileName(obsPath: ObsPath): String = {
    getPath(obsPath.pathWithoutScheme) match {
      case Success(path) => path.getFileName.pathAsString
      case _ => obsPath.pathWithoutScheme
    }
  }

  private[batch] def localizeObsPath(obsPath: ObsPath): String = {
    if (isOutputObsFileString(obsPath.pathAsString) && !obsPath.isAbsolute) {
      if (obsPath.exists) {
        obsPathToMount(obsPath).dest.normalize().pathAsString
      } else {
        commandDirectory.resolve(getObsFileName(obsPath)).normalize().pathAsString
      }
    } else {
      userDefinedMounts collectFirst {
        case huaweiBatchMount: HuaweiBatchMount if obsPath.pathAsString.startsWith(huaweiBatchMount.src.pathAsString) =>
          huaweiBatchMount.dest.resolve(obsPath.pathAsString.stripPrefix(huaweiBatchMount.src.pathAsString)).pathAsString
      } getOrElse {
        val mount = obsPathToMount(obsPath)
        mount.dest.pathAsString
      }
    }
  }

  override def mapCommandLineWomFile(womFile: WomFile): WomFile = {
    getPath(womFile.valueString) match {
      case Success(obsPath: ObsPath) =>
        WomFile(WomSingleFileType, localizeObsPath(obsPath))
      case Success(path: Path) if !path.isAbsolute =>
        WomFile(WomSingleFileType, relativeOutputPath(path).pathAsString)
      case _ => womFile
    }
  }

  override def isTerminal(runStatus: RunStatus): Boolean = {
    runStatus match {
      case _: TerminalRunStatus => true
      case _ => false
    }
  }

  override def getTerminalEvents(runStatus: RunStatus): Seq[ExecutionEvent] = {
    runStatus match {
      case successStatus: SUCCEEDED => successStatus.eventList
      case unknown =>
        throw new RuntimeException(s"handleExecutionSuccess not called with RunStatus.Success. Instead got $unknown")
    }
  }

  override def handleExecutionFailure(runStatus: RunStatus,
                                      returnCode: Option[Int]): Future[ExecutionHandle] = {
    runStatus match {
      case RunStatus.FAILED(jobId, Some(errorMessage), _) =>
        val exception = new Exception(s"Job id $jobId failed: '$errorMessage'")
        Future.successful(FailedNonRetryableExecutionHandle(exception, returnCode))
      case _ => super.handleExecutionFailure(runStatus, returnCode)
    }
  }

  override def isDone(runStatus: RunStatus): Boolean = {
    runStatus match {
      case _: SUCCEEDED =>
        //        runtimeAttributes.autoReleaseJob match {
        //          case Some(true) | None =>
        //            huaweiBatchClient.deleteJob(runStatus.jobId)
        //          case _ =>TODO:hy
        //        }
        true
      case _ => false
    }
  }

  private[batch] lazy val rcHuaweiBatchOutput = HuaweiBatchOutputMount(
    commandDirectory.resolve(huaweiBatchJobPaths.returnCodeFilename), huaweiBatchJobPaths.returnCode, writeSupport = false)

  private[batch] lazy val stdoutHuaweiBatchOutput = HuaweiBatchOutputMount(
    commandDirectory.resolve(huaweiBatchJobPaths.defaultStdoutFilename), standardPaths.output, writeSupport = false)
  private[batch] lazy val stderrHuaweiBatchOutput = HuaweiBatchOutputMount(
    commandDirectory.resolve(huaweiBatchJobPaths.defaultStderrFilename), standardPaths.error, writeSupport = false)

  override def executeAsync(): Future[ExecutionHandle] = {
    commandScriptContents.fold(
      errors => Future.failed(new RuntimeException(errors.toList.mkString(", "))),
      huaweiBatchJobPaths.script.write)

    val script = s"cp ${commandDirectory.resolve(huaweiBatchJobPaths.scriptFilename)} /run.sh;. /run.sh;cat $rcPath|sed '/^\\s*$$/d' > $rcPath"

    val huaweiBatchJob = new HuaweiBatchJob(
      jobName,
      jobTag,
      instantiatedCommand.commandString,
      huaweiBatchMounts,
      huaweiBatchEnvs,
      script.toString,
      runtimeAttributes,
      Some(huaweiBatchJobPaths.huaweiBatchStdoutPath),
      Some(huaweiBatchJobPaths.huaweiBatchStderrPath),
      huaweiBatchClient
    )
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

  override def mapOutputWomFile(wdlFile: WomFile): WomFile = {
    wdlFileToObsPath(generateHuaweiBatchOutputs(jobDescriptor))(wdlFile)
  }

  private[batch] def wdlFileToObsPath(huaweiBatchOutputs: Seq[HuaweiBatchMount])(wdlFile: WomFile): WomFile = {
    huaweiBatchOutputs collectFirst {
      case huaweiBatchOutput if huaweiBatchOutput.src.pathAsString.endsWith(wdlFile.valueString) => WomFile(WomSingleFileType, huaweiBatchOutput.dest.pathAsString)
    } getOrElse wdlFile
  }

  override def tryAbort(job: StandardAsyncJob): Unit = {
    for {
      client <- Try(initializationData.huaweiBatchConfiguration.huaweiBatchClient getOrElse (throw new RuntimeException("empty run job info ")))
      resp <- Try(client.getJob(job.jobId))
      status <- RunStatusFactory.getStatus(job.jobId, resp.getState)
    } yield {
      status match {
        case _: RunStatus.TerminalRunStatus =>
          for {
            _ <- Try(client.deleteJob(job.jobId))
          } yield job
        case _ =>
          for {
            _ <- Try(client.terminateJob(job.jobId))
            _ <- Try(client.deleteJob(job.jobId))
          } yield job
      }
    }
    ()
  }

  override def isFatal(throwable: Throwable): Boolean = super.isFatal(throwable) || isFatalHuaweiBatchException(throwable)

  private[batch] def isFatalHuaweiBatchException(throwable: Throwable) = {
    throwable match {
      case e: ApiException if e.getCode >= 400 && e.getCode < 500 => true
      case _ => false
    }
  }

  override def isTransient(throwable: Throwable): Boolean = {
    throwable match {
      case e: ApiException if e.getCode >= 400 && e.getCode < 600 => true
      case _ => false
    }
  }

  private[batch] lazy val huaweiBatchEnvs: Map[String, String] = Map(
    HuaweiBatchJobPaths.HuaweiBatchEnvCwdKey -> commandDirectory.pathAsString,
    HuaweiBatchJobPaths.HuaweiBatchEnvExecKey -> huaweiBatchJobPaths.script.pathAsString,
    HuaweiBatchJobPaths.HuaweiBatchEnvStdoutKey -> commandDirectory.resolve(huaweiBatchJobPaths.defaultStdoutFilename).pathAsString,
    HuaweiBatchJobPaths.HuaweiBatchEnvStderrKey -> commandDirectory.resolve(huaweiBatchJobPaths.defaultStderrFilename).pathAsString,
    HuaweiBatchConfiguration.ObsEndpointKey -> huaweiBatchConfiguration.obsEndpoint,
    HuaweiBatchConfiguration.ObsAccessKey -> huaweiBatchConfiguration.obsAccessKey,
    HuaweiBatchConfiguration.ObsSecretKey -> huaweiBatchConfiguration.obsSecretKey
  )

  private[batch] lazy val huaweiBatchMounts: Seq[HuaweiBatchMount] = {
    generateHuaweiBatchInputs(jobDescriptor)
    runtimeAttributes.mounts.getOrElse(Seq.empty) ++ inputMounts ++
      generateHuaweiBatchOutputs(jobDescriptor) :+ huaweiBatchWorkflowInputMount
  }

}
