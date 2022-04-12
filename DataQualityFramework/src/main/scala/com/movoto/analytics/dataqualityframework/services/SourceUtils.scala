package com.movoto.analytics.dataqualityframework.services

import com.movoto.analytics.dataqualityframework.ops.SparkOps
import com.movoto.analytics.dataqualityframework.conf.AppConfig

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SourceUtils extends AppConfig {

  //Declaring spark object
  lazy val spark:SparkSession = SparkOps.getSparkSession(config.APP_NAME)

  /**
   * Method to extract data from AWS S3 storage
   * @param inputPath s3 bucket source path from where the data has to be read
   * @return Spark Dataframe which stored the extracted data from S3 bucket
   */
  def extractSourceData(fileType:String = "parquet", sourcePath:String, sourceOptions: Map[String,String] ): DataFrame = {
    logger.info("""Creating Source Data frame by extracting Source data""")
    if (sourceOptions.isEmpty) {
      spark.read.format(fileType).load(sourcePath)
    }
    else {
      spark.read.format(fileType).options(sourceOptions).load(sourcePath)
    }
  }

  def runDQCheck(checkType: String): Unit = {
    if (checkType == "NULL_CHECK") {
      NullCheck()
    }
    if (checkType == "COUNT_CHECK") {
      val countCheck = new CountCheck()
      countCheck.countCheck()
    }
    if (checkType == "DISTINCT_COUNT_CHECK") {
      DistinctCountCheck()
    }
    if (checkType == "AVERAGE_CHECK") {
      AverageCheck()
    }
    if (checkType == "MINIMUM_CHECK") {
      MinimumCheck()
    }
    if (checkType == "MAXIMUM_CHECK") {
      MaximumCheck()
    }
    if (checkType == "SUM_CHECK") {
      SumCheck()
    }
    if (checkType == "QUALITY_CHECK") {
      QualityCheck()
    }
    if (checkType == "AGGREGATIONS_CHECK") {
      AggregationsCheck()
    }
  }
  }
}
