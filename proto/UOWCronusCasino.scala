// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC ### UOW Cronus Casino

// COMMAND ----------

import com.kaizengaming.de.pltf.common.utils.spark.SparkUtils._
import org.apache.spark.sql.functions.max

// COMMAND ----------

import com.kaizengaming.de.pltf.common.utils.etl.loaders.strategies.configs._
import com.kaizengaming.de.pltf.common.utils.operatorsconfig.OperatorsConfig

// COMMAND ----------

// MAGIC %run /Repos/de-prd/common/utils/jobMonitoring/NotebookParExecutor

// COMMAND ----------

// MAGIC %run ../silver/cronus/configs/CasinoRoundInfoConfigurations

// COMMAND ----------

val timeout = widget("timeout").orDefault("1800").toInt
val jobProfile = widget("job_profile").orDefault("prd")
val logExecInPRD = jobProfile.equals("prd")
// val startingOffsets = widget("startingOffsets").orDefault("latest")
val startingOffsets = "earliest"

assert(Seq("earliest", "latest").contains(startingOffsets), "Starting offsets, can be either 'earliest' or 'latest' for this UOW")

// COMMAND ----------

// Get Metadata about active companies for Cronus
val releasedCronusCasino = OperatorsConfig.fromConfigFile(sys.env("KAIZEN_DE_PATH_CONFIG_DE_PLATFORM")).releasedCronusCasino
val activeCronusCompanies: List[(String, String)] = releasedCronusCasino.map({case (id, company, kafkaProfile) => (company, kafkaProfile)})

val commonPath = sys.env("KAIZEN_DE_PLTF_NB_COMMON_PATH")
val loadKafkaStreamToDeltaNbPath = commonPath + "/utils/loadStrategies/kafka/LoadKafkaStreamToDelta"

// COMMAND ----------

// MAGIC %md
// MAGIC LoadStreamToDelta

// COMMAND ----------

val sourceOptions = Some(Map(
    "startingOffsets" -> startingOffsets,
    "maxOffsetsPerTrigger" -> "1000000"
  ))

// COMMAND ----------

// Open Rounds
val openRoundsNbParamsPerCompany = activeCronusCompanies.map{
  case(company, kafkaProfile) =>
    LoadKafkaStreamToDeltaOptions(
      sourceProfile  = kafkaProfile,
      sourceTopic = s"cronus.casino.openrounds.out.${company}",
      sourceCheckPointPath = sys.env("KAIZEN_KAFKA_CHECKPOINT_VOLUME_PATH") + s"/cronus/_checkpoints_cronuscore_ocpprd3/cronus_raw/casino_open_round_info_${company}",
      sourceOptions = sourceOptions,
      sinkDbName = "cronus_raw",
      sinkTablePath = "",
      sinkTableName = s"casino_open_round_info_${company}",
      sinkPartitions = Some(List(("p_yearMonth", "cast( date_format(timestamp,'yyyyMM') as INT)"))),
      sinkTriggerType = "AvailableNow"
    )
}.map { x =>
      Map(
        "jobName" -> s"${x.sinkDbName}.${x.sinkTableName}",
        "strategyConfig" -> x.toJson
      )
}

// COMMAND ----------

// Closed Rounds
val closedRoundsNbParamsPerCompany = activeCronusCompanies.map{
  case(company, kafkaProfile) =>
    LoadKafkaStreamToDeltaOptions(
      sourceProfile  = kafkaProfile,
      sourceTopic = s"cronus.casino.out.${company}",
      sourceCheckPointPath = sys.env("KAIZEN_KAFKA_CHECKPOINT_VOLUME_PATH") + s"/cronus/_checkpoints_cronuscore_ocpprd3/cronus_raw/casino_closed_round_info_${company}",
      sourceOptions = sourceOptions,
      sinkDbName = "cronus_raw",
      sinkTablePath = "",
      sinkTableName = s"casino_closed_round_info_${company}",
      sinkPartitions = Some(List(("p_yearMonth", "cast( date_format(timestamp,'yyyyMM') as INT)"))),
      sinkTriggerType = "AvailableNow"
    )
}.map { x =>
      Map(
        "jobName" -> s"${x.sinkDbName}.${x.sinkTableName}",
        "strategyConfig" -> x.toJson
      )
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Cronus tables

// COMMAND ----------

// LoadKafkaStreamToDelta - Per Company
val openRoundsPerCompanyNb = openRoundsNbParamsPerCompany.map(NotebookData(loadKafkaStreamToDeltaNbPath, timeout, _))
val closedRoundsPerCompanyNb = closedRoundsNbParamsPerCompany.map(NotebookData(loadKafkaStreamToDeltaNbPath, timeout, _))

// COMMAND ----------

// CasinoRoundInfo - Per Company
val casinoRoundInfoPerCompanyNb = casinoRoundInfoNbParamsPerCompany.map(NotebookData("../silver/cronus/utils/CronusInfoStrategy", timeout, _))

// COMMAND ----------

// MAGIC %md
// MAGIC ####Execution Step

// COMMAND ----------

val bronzeStep = ExecutionStep(
  notebooks =
  openRoundsPerCompanyNb ++
  closedRoundsPerCompanyNb,
  parallelism = activeCronusCompanies.size,
  notebookTimeout = timeout
  )

// COMMAND ----------

val silverStep = ExecutionStep(
  notebooks = casinoRoundInfoPerCompanyNb,
  parallelism = 2,
  notebookTimeout = timeout
  )

// COMMAND ----------

dryRun()

// COMMAND ----------

val execution = ExecutionPlan(
  executionSteps=Seq(bronzeStep, silverStep),
  pipelineName="UOWCronusNew",
  // this feeds into isPrd
  logExecInPRD=logExecInPRD
)
val (successfullSteps, failedNotebooks) = execution.run()

// COMMAND ----------

if(!failedNotebooks.isEmpty) {
  throw new RuntimeException(s"""The following tables have failed to extract ${failedNotebooks.map(_.notebook.parameters.get("jobName")).mkString(", ")}""")
}

// COMMAND ----------

dbutils.notebook.exit("SUCCESS")
