package com.movoto.analytics.dataqualityframework.services

import com.movoto.analytics.dataqualityframework.ops.SparkOps.logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.countDistinct

class DistinctCountCheck {
  /**
   * Method to validate the distinct counts of selected column between two sources
   * @param sourceDataFrame : source data
   * @param targetDataFrame : target data
   */
  def distinctCountCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, sourceColumn: String, targetColumn: String): Unit = {
    if(sourceDataFrame.select(countDistinct(sourceColumn)).collect()(0)(0) == targetDataFrame.select(countDistinct(targetColumn)).collect()(0)(0)){
      logger.info("INFO: Distinct Count is matching")
    }
    else {
      logger.warn("WARN: Distinct Count is not matching. Please check")
    }
  }

}
