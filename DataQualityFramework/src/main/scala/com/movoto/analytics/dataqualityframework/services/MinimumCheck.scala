package com.movoto.analytics.dataqualityframework.services

import com.movoto.analytics.dataqualityframework.ops.SparkOps.logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.min

class MinimumCheck {
  /**
   * Method to validate the minimum value of selected column between two sources
   * @param sourceDataFrame : source data
   * @param targetDataFrame : target data
   */
  def maximumCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, sourceColumn: String, targetColumn: String): Unit = {
    if(sourceDataFrame.select(min(sourceColumn)).collect()(0)(0) == targetDataFrame.select(min(targetColumn)).collect()(0)(0)){
      logger.info("INFO: Minimum is matching")
    }
    else {
      logger.warn("WARN: Minimum is not matching. Please check")
    }
  }
}
