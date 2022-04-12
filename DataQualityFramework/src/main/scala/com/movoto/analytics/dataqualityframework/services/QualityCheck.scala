package com.movoto.analytics.dataqualityframework.services

import com.movoto.analytics.dataqualityframework.ops.SparkOps.logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, hash}

class QualityCheck {
  /**
   * Method to validate the quality of records between two sources
   * @param sourceDataFrame : source data
   * @param targetDataFrame : target data
   */
  def qualityCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, targetColumn: String): Unit = {
    // Hashing the records of both source and target
    val hashedSource: DataFrame = sourceDataFrame.withColumn("hash", hash(sourceDataFrame.columns.map(col): _*))
    val hashedTarget: DataFrame = targetDataFrame.withColumn("hash", hash(targetDataFrame.columns.map(col): _*))

    // Joining the datasets with hash value, if the target do not have nulls, then the data is qualified
    // else 10 sample records will be displayed in logs
    if (hashedSource.join(hashedTarget, hashedSource("hash") === hashedTarget("hash"), "left")
      .filter(hashedTarget(targetColumn).isNull).count() == 0) {
      logger.info("INFO: Qualified data")
    }
    else {
      logger.warn("WARN: Data is not qualified.Please check below sample records")
      hashedSource.join(hashedTarget, hashedSource("hash") === hashedTarget("hash"), "left")
        .filter(hashedTarget(targetColumn).isNull).show(10,truncate = false)
    }
  }
}
