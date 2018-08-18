package cromwell.backend.impl.huawei.batch

import cromwell.core.ExecutionEvent
import scala.util.Try
import org.slf4j.LoggerFactory
import com.huawei.batch.client.ApiException
import com.huawei.batch.client.BatchApi
import com.huawei.batch.model.ComputeEnv
import com.huawei.batch.model.job.{JobAddParameter, JobInfo, JobState}
import com.huawei.batch.model.task.TaskAddParameter
import com.huawei.batch.model.task.TaskDefinition
import com.huawei.batch.model.LogRedirectPath
import com.huawei.batch.model.RetryPolicy
import com.huawei.batch.model.task.TaskDocker
import com.huawei.batch.model.task.TaskResourceAffinity
import com.huawei.batch.model.task.TaskResourceLimit
import cromwell.core.path.Path

//TODO: add batch client
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

  private[batch] def taskAddParameters: java.util.List[TaskAddParameter] = {
    val resourceAffinity = new TaskResourceAffinity
    resourceAffinity.setPoolId("poolx2l01l9q")

    val logRedirectPath = new LogRedirectPath
    logRedirectPath.setStderrRedirectPath("")
    logRedirectPath.setStdoutRedirectPath("")

    val resourceLimit = new TaskResourceLimit
    resourceLimit.setCpu(1)
    resourceLimit.setMem(512)

    val retryPolicy = new RetryPolicy
    retryPolicy.setMaxRetryCnt(1)

    val docker = new TaskDocker
    docker.setImage("iva/ubuntu:16.04")
    //docker.setServer("100.125.5.235:20202/")
    val taskDefinition = new TaskDefinition

    taskDefinition.setTaskNamePrefix("sdk-task-1-1")
    taskDefinition.setDescription("created by sdk")
    taskDefinition.setCmd("sleep 3")
    //taskDefinition.setCmd(script)
    taskDefinition.setResourceLimit(resourceLimit)
    taskDefinition.setRetryPolicy(retryPolicy)
    taskDefinition.setDocker(docker)
    taskDefinition.setResourceAffinity(resourceAffinity)
    taskDefinition.setLogRedirectPath(logRedirectPath)
    val taskAddParameter = new TaskAddParameter
    taskAddParameter.setTaskDefinition(taskDefinition)
    taskAddParameter.setTaskGroupName("sdk-task-1")
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
    jobAddParameter.setJobName("sdk-job")
      .setDescription("created by sdk")
      .setMaxTimeout(0)
      .setPriority(5)
      .setScheduledTime("")
      .setTasks(taskAddParameters)
      .setComputeEnvs(computeEnvs)
      .setSyncStart(false)
    //.jobAddParameter.setDag(dag);
  }

  def submit(): Try[String] = Try {
    val logger = LoggerFactory.getLogger("HuaweiBatchJob")
    var jobId: String = "__jobId";
    try {
      jobId = batchApi.addJob(jobAddParameter)
    } catch {
      case ex: ApiException => {
        ex.printStackTrace()
      }
    }
    logger.warn(jobId)
    logger.warn("HuaweiBacthJob.submit()")
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
