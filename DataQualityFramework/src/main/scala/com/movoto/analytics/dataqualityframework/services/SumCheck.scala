package com.movoto.analytics.dataqualityframework.services

import com.movoto.analytics.dataqualityframework.ops.SparkOps.logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.sum

class SumCheck {
  /**
   * Method to validate the sum of selected column between two sources
   * @param sourceDataFrame : source data
   * @param targetDataFrame : target data
   */
  def maximumCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, sourceColumn: String, targetColumn: String): Unit = {
    if(sourceDataFrame.select(sum(sourceColumn)).collect()(0)(0) == targetDataFrame.select(sum(targetColumn)).collect()(0)(0)) {
      logger.info("INFO: Sum is matching")
    }
    else {
      logger.warn("WARN: Sum is not matching. Please check")
    }
  }
}
