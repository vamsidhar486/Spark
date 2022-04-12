package com.movoto.analytics.dataingestionframework.conf

import com.typesafe.config.Config
import com.movoto.analytics.dataingestionframework.ops.AppLogger
import com.movoto.analytics.dataingestionframework.constants.ConfConstants

import scala.collection.JavaConverters._

class ConfValues(conf: Config) extends AppLogger {

  val APP_NAME: String = conf.getString(ConfConstants.APP_NAME).toUpperCase.trim
  val MIGRATION_TYPE: String = conf.getString(ConfConstants.MIGRATION_TYPE).toUpperCase.trim

  val SOURCE: String = conf.getString(ConfConstants.SOURCE).toUpperCase.trim
  val SOURCE_INPUT: String = conf.getString(ConfConstants.SOURCE_INPUT).stripMargin.trim
  val DATA_FORMAT: String = conf.getString(ConfConstants.DATA_FORMAT).trim
  val INPUT_OPTIONS: Map[String,String] = (conf.getStringList(ConfConstants.INPUT_OPTIONS_KEYS).asScala.toList zip conf.getStringList(ConfConstants.INPUT_OPTIONS_VALUES).asScala.toList).toMap

  val TARGET: String = conf.getString(ConfConstants.TARGET).toUpperCase.trim
  val INGESTION_COMPLEXITY: String = conf.getString(ConfConstants.INGESTION_COMPLEXITY).toUpperCase.trim
  val NUM_PARTITIONS: Int = conf.getInt(ConfConstants.NUM_PARTITIONS)
  val MODE_TYPE: String = conf.getString(ConfConstants.MODE_TYPE).toLowerCase.trim
  val PARTITION_COLUMNS: List[String] = conf.getStringList(ConfConstants.PARTITION_COLUMNS).asScala.toList
  val OUTPUT_FORMAT: String = conf.getString(ConfConstants.OUTPUT_FORMAT).toLowerCase.trim
  val OUTPUT_OPTIONS: Map[String,String] = (conf.getStringList(ConfConstants.OUTPUT_OPTIONS_KEYS).asScala.toList zip conf.getStringList(ConfConstants.OUTPUT_OPTIONS_VALUES).asScala.toList).toMap
  val TARGET_OUTPUT: String = conf.getString(ConfConstants.TARGET_OUTPUT).trim

  val SPARK_CONF: Seq[String] = conf.getStringList(ConfConstants.SPARK_CONFIG).asScala

}
