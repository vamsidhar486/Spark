package com.movoto.analytics.dataqualityframework.services

import com.movoto.analytics.dataqualityframework.ops.SparkOps.logger
import org.apache.spark.sql.DataFrame

class CountCheck {
  /**
   * Method to validate the counts between two sources
   * @param sourceDataFrame : source data
   * @param targetDataFrame : target data
   */
  def countCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame): Unit = {
    if(sourceDataFrame.count() == targetDataFrame.count()){
      logger.info("INFO: Count is matching")
    }
    else {
      logger.warn("WARN: Count is not matching.Please check")
    }
  }
}

