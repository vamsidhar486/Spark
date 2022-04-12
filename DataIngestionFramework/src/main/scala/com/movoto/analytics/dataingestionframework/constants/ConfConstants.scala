package com.movoto.analytics.dataingestionframework.constants

object ConfConstants extends Serializable {

  val APP_NAME = "movoto.analytics.etl.appName"
  val MIGRATION_TYPE = "movoto.analytics.etl.migrationType"

  val SOURCE = "movoto.analytics.etl.source.type"
  val SOURCE_INPUT = "movoto.analytics.etl.source.input"
  val DATA_FORMAT = "movoto.analytics.etl.source.format"
  val INPUT_OPTIONS_KEYS = "movoto.analytics.etl.source.inputOptionKeys"
  val INPUT_OPTIONS_VALUES = "movoto.analytics.etl.source.inputOptionValues"

  val TARGET = "movoto.analytics.etl.target.type"
  val INGESTION_COMPLEXITY = "movoto.analytics.etl.target.ingestionComplexity"
  val NUM_PARTITIONS = "movoto.analytics.etl.target.numPartitions"
  val MODE_TYPE = "movoto.analytics.etl.target.modeType"
  val PARTITION_COLUMNS = "movoto.analytics.etl.target.partitionColumns"
  val OUTPUT_FORMAT = "movoto.analytics.etl.target.format"
  val OUTPUT_OPTIONS_KEYS = "movoto.analytics.etl.target.outputOptionKeys"
  val OUTPUT_OPTIONS_VALUES = "movoto.analytics.etl.target.outputOptionValues"
  val TARGET_OUTPUT = "movoto.analytics.etl.target.outputPath"

  val SPARK_CONFIG = "movoto.analytics.etl.sparkConf"

}

