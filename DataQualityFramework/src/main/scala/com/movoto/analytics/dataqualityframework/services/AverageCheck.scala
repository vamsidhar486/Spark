package com.movoto.analytics.dataqualityframework.services

import com.movoto.analytics.dataqualityframework.ops.SparkOps.logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.avg

class AverageCheck {
  /**
   * Method to validate the average of selected column between two sources
   * @param sourceDataFrame : source data
   * @param targetDataFrame : target data
   */
  def averageCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, sourceColumn: String, targetColumn: String): Unit = {
    if(sourceDataFrame.select(avg(sourceColumn)).collect()(0)(0) == targetDataFrame.select(avg(targetColumn)).collect()(0)(0)){
      logger.info("INFO: Average is matching")
    }
    else {
      logger.warn("WARN: Average is not matching. Please check")
    }
  }
}
