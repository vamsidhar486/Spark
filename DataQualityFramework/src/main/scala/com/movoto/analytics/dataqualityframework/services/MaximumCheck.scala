package com.movoto.analytics.dataqualityframework.services

import com.movoto.analytics.dataqualityframework.ops.SparkOps.logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.max

class MaximumCheck {
  /**
   * Method to validate the maximum value of selected column between two sources
   * @param sourceDataFrame : source data
   * @param targetDataFrame : target data
   */
  def maximumCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, sourceColumn: String, targetColumn: String): Unit = {
    if(sourceDataFrame.select(max(sourceColumn)).collect()(0)(0) == targetDataFrame.select(max(targetColumn)).collect()(0)(0)){
      logger.info("INFO: Maximum is matching")
    }
    else {
      logger.warn("WARN: Maximum is not matching. Please check")
    }
  }
}
