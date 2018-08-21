package cromwell.backend.impl.huawei.batch

import cromwell.core.ExecutionEvent

import scala.util.Try
import org.slf4j.LoggerFactory
import com.huawei.batch.client.ApiException
import com.huawei.batch.client.BatchApi
import com.huawei.batch.model.{ComputeEnv, LogRedirectPath, RetryPolicy, MountPath}
import com.huawei.batch.model.job.{JobAddParameter, JobInfo, JobState}
import com.huawei.batch.model.task.TaskAddParameter
import com.huawei.batch.model.task.TaskDefinition
import com.huawei.batch.model.task.TaskDocker
import com.huawei.batch.model.task.TaskResourceAffinity
import com.huawei.batch.model.task.TaskResourceLimit
import cromwell.core.path.Path

final case class HuaweiBatchJob(name: String,
                                description: String,
                                commandString: String,
                                mounts: Seq[HuaweiBatchMount],
                                envs: Map[String, String],
                                script: String,
                                runtime: HuaweiBatchRuntimeAttributes,
                                stdoutPath: Option[Path],
                                stderrPath: Option[Path],
                                batchApi: BatchApi) {

  lazy val lazyTask: java.util.List[TaskAddParameter] = new java.util.ArrayList
  val logger = LoggerFactory.getLogger("HuaweiBatchJob")

  private[batch] def taskAddParameters: java.util.List[TaskAddParameter] = {
    val resourceAffinity = new TaskResourceAffinity
    resourceAffinity.setPoolId("poolx2l01l9q")

    val logRedirectPath = new LogRedirectPath
    stdoutPath foreach { path => logRedirectPath.setStderrRedirectPath(path.normalize().pathAsString + "/") }
    stderrPath foreach { path => logRedirectPath.setStdoutRedirectPath(path.normalize().pathAsString + "/") }

    val resourceLimit = new TaskResourceLimit
    resourceLimit.setCpu(1)
    resourceLimit.setMem(512)

    val retryPolicy = new RetryPolicy
    retryPolicy.setMaxRetryCnt(1)

    val docker = new TaskDocker
    docker.setImage("iva/ubuntu:16.04")
    //docker.setServer("100.125.5.235:20202/")
    val taskDefinition = new TaskDefinition

    taskDefinition.setTaskNamePrefix("cromwell")
    taskDefinition.setDescription("created by sdk")
    taskDefinition.setCmd(script)
    taskDefinition.setResourceLimit(resourceLimit)
    taskDefinition.setRetryPolicy(retryPolicy)
    taskDefinition.setDocker(docker)
    taskDefinition.setResourceAffinity(resourceAffinity)
    taskDefinition.setLogRedirectPath(logRedirectPath)
    //taskDefinition.setEnv(environments.asJava)

    mounts foreach {
      case input: HuaweiBatchInputMount =>
        var destStr = input.dest.pathAsString
        if (input.src.pathAsString.endsWith("/") && !destStr.endsWith("/")) {
          destStr += "/"
        }
        logger.warn(input.src.pathAsString)
        logger.warn(destStr)
        taskDefinition.addInputs(new MountPath(input.src.pathAsString, destStr))
      case output: HuaweiBatchOutputMount =>
        var srcStr = output.src.pathAsString
        if (output.dest.pathAsString.endsWith("/") && !srcStr.endsWith("/")) {
          srcStr += "/"
        }
        logger.warn(srcStr)
        logger.warn(output.dest.pathAsString)
        taskDefinition.addOutputs(new MountPath(srcStr, output.dest.pathAsString))
    }

    val taskAddParameter = new TaskAddParameter
    taskAddParameter.setTaskDefinition(taskDefinition)
    taskAddParameter.setTaskGroupName("cromwell-group")
    taskAddParameter.setReplicas(1)

    lazyTask.add(taskAddParameter);
    lazyTask
  }

  lazy val lazyJob = new JobAddParameter

  private[batch] def jobAddParameter: JobAddParameter = {
    val computeEnv: ComputeEnv = new ComputeEnv()
    computeEnv.setPoolId("poolx2l01l9q")

    val computeEnvs: java.util.List[ComputeEnv] = new java.util.ArrayList()
    computeEnvs.add(computeEnv)

    val jobAddParameter: JobAddParameter = new JobAddParameter()
    jobAddParameter.setJobName(name)
      .setDescription(description)
      .setMaxTimeout(0)
      .setPriority(5)
      .setScheduledTime("")
      .setTasks(taskAddParameters)
      .setComputeEnvs(computeEnvs)
      .setSyncStart(false)
    //.jobAddParameter.setDag(dag);
  }

  def submit(): Try[String] = Try {

    var jobId: String = "__jobId";
    try {
      jobId = batchApi.addJob(jobAddParameter)
    } catch {
      case ex: ApiException => {
        logger.error(ex.getCode.toString)
        logger.error(ex.getResponseBody)
        //ex.printStackTrace()
      }
    }
    logger.warn("HuaweiBacthJob.submit()")
    logger.warn(jobId)
    jobId
  }


  def getStatus(jobId: String): Try[RunStatus] = {
    val jobInfo: JobInfo = batchApi.getJob(jobId)
    val jobState: JobState = jobInfo.getState()
    val message = String.join("\n", jobInfo.getTraceMessages)
    val eventList = Seq[ExecutionEvent]()
    RunStatusFactory.getStatus(jobId, jobState, Some(message), Some(eventList))
  }
}
