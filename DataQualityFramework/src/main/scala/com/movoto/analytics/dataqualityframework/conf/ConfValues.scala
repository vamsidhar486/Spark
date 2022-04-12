package com.movoto.analytics.dataqualityframework.conf

import com.typesafe.config.Config
import com.movoto.analytics.dataqualityframework.ops.AppLogger
import com.movoto.analytics.dataqualityframework.constants.ConfConstants

import scala.collection.JavaConverters._

/**
 * Main entry point for type safe configuration values
 * @param conf configuration file passed during spark-submit
 */
class ConfValues(conf: Config) extends AppLogger {

  val APP_NAME: String = conf.getString(ConfConstants.APP_NAME).toUpperCase.trim

  val MLS_LISTING_INPUT_S3: String = conf.getString(ConfConstants.MLS_LISTING_INPUT_S3).trim
  val ADDRESS_S3: String = conf.getString(ConfConstants.ADDRESS_S3).trim
  val ATTRIBUTE_S3: String = conf.getString(ConfConstants.ATTRIBUTE_S3).trim
  val PUBLIC_RECORD_ASSOCIATION_S3: String = conf.getString(ConfConstants.PUBLIC_RECORD_ASSOCIATION_S3).trim
  val IMAGE_DOWNLOAD_S3: String = conf.getString(ConfConstants.IMAGE_DOWNLOAD_S3).trim
  val ZIPCODE_USPS_S3: String = conf.getString(ConfConstants.ZIPCODE_USPS_S3).trim
  val PHOTO_COVERAGE_S3: String = conf.getString(ConfConstants.PHOTO_COVERAGE_S3).trim
  val S3_OUTPUT_DEV: String = conf.getString(ConfConstants.S3_OUTPUT_DEV).trim

  val PHOTO_COVERAGE_SQL: String = conf.getString(ConfConstants.PHOTO_COVERAGE_SQL).stripMargin
  val PHOTO_STATS_SUM_SQL: String = conf.getString(ConfConstants.PHOTO_STATS_SUM_SQL).stripMargin
  val PHOTO_STATS_AVG_SQL: String = conf.getString(ConfConstants.PHOTO_STATS_AVG_SQL).stripMargin
  val NO_IMAGE_LISTING_PERCENTAGE_SQL: String = conf.getString(ConfConstants.NO_IMAGE_LISTING_PERCENTAGE_SQL).stripMargin

  val ES_PHOTO_STATS_SUM_INDEX_TYPE: String = conf.getString(ConfConstants.ES_PHOTO_STATS_SUM_INDEX_TYPE).trim
  val ES_PHOTO_STATS_AVG_INDEX_TYPE: String = conf.getString(ConfConstants.ES_PHOTO_STATS_AVG_INDEX_TYPE).trim
  val ES_NO_IMAGE_LISTING_PERCENTAGE_INDEX_TYPE: String = conf.getString(ConfConstants.ES_NO_IMAGE_LISTING_PERCENTAGE_INDEX_TYPE).trim

  val SPARK_CONF: Seq[String] = conf.getStringList(ConfConstants.SPARK_CONFIG).asScala

}
