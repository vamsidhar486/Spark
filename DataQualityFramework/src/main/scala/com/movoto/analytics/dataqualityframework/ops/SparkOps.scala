package com.movoto.analytics.dataqualityframework.ops

import com.movoto.analytics.dataqualityframework.conf.AppConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Object which has the definitions of spark basic operations
 */
object SparkOps extends AppConfig {

  /**
   * This function allows to create a spark session
   * @param appName will be the name of the application
   * @return SparkSession object
   */
  def getSparkSession(appName: String): SparkSession = {
    SparkSession
      .builder()
      .config(getSparkConf)
      .appName(appName)
      .getOrCreate()
  }

  /**
   * This Function will read the configuration properties passed in application.conf
   * @return conf object with configuration properties
   */
  def getSparkConf: SparkConf = {
    val conf: SparkConf = new SparkConf()
    config.SPARK_CONF
      .filter(_.nonEmpty)
      .map(_.split('=').map(_.trim))
      .filter(_.length == 2)
      .map(x => {
        conf.set(x.head, x.tail.head)
      })
    conf
  }

}
