package com.movoto.analytics.dataqualityframework.services

import com.movoto.analytics.dataqualityframework.ops.SparkOps.logger
import org.apache.spark.sql.DataFrame

class NullCheck {
  /**
   * Method to validate the presence of Null value for a selected column between two sources
   * @param sourceDataFrame : source data
   * @param targetDataFrame : target data
   */
  def countCheck(sourceDataFrame: DataFrame, sourceColumn: String): Unit = {
    if(sourceDataFrame.filter(sourceDataFrame(sourceColumn).isNull).count() == 0){
      logger.info("INFO: Null Check is passed")
    }
    else {
      logger.warn("WARN: Column contains null values.Please check below sample records")
      sourceDataFrame.filter(sourceDataFrame(sourceColumn).isNull).show(10, truncate = false)
    }
  }
}
