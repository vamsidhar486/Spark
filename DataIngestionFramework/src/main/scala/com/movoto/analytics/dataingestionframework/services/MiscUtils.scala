package com.movoto.analytics.dataingestionframework.services

import org.apache.spark.sql.DataFrame

object MiscUtils extends GenericUtils {

  //  val sdv: SDVCheck = new SDVCheck()

  override def runDataIngestion(source: String, target: String): Unit = {
    if (source == "HIVE" && target == "S3") {
      val dataFrame: DataFrame = extractHiveData(config.SOURCE_INPUT)
      loadDataToS3(config.INGESTION_COMPLEXITY, dataFrame, config.NUM_PARTITIONS, config.MODE_TYPE, config.PARTITION_COLUMNS, config.OUTPUT_FORMAT, config.TARGET_OUTPUT, config.OUTPUT_OPTIONS)
      //      sdv.SDVCheck(dataFrame, sdv.recordsWrittenCount)
    }
    else if (source == "HDFS" && target == "S3") {
      val dataFrame: DataFrame = extractHDFSData(config.SOURCE_INPUT, config.DATA_FORMAT)
      loadDataToS3(config.INGESTION_COMPLEXITY, dataFrame, config.NUM_PARTITIONS, config.MODE_TYPE, config.PARTITION_COLUMNS, config.OUTPUT_FORMAT, config.TARGET_OUTPUT, config.OUTPUT_OPTIONS)
      //      sdv.SDVCheck(dataFrame, sdv.recordsWrittenCount)
    }
    else if (source == "S3" && target == "S3") {
      val dataFrame: DataFrame = extractS3Data(config.SOURCE_INPUT, config.DATA_FORMAT)
      loadDataToS3(config.INGESTION_COMPLEXITY, dataFrame, config.NUM_PARTITIONS, config.MODE_TYPE, config.PARTITION_COLUMNS, config.OUTPUT_FORMAT, config.TARGET_OUTPUT, config.OUTPUT_OPTIONS)
      //      sdv.SDVCheck(dataFrame, sdv.recordsWrittenCount)
    }
    //    else if(source == "RDBMS" && target == "S3") {
    //      val dataFrame: DataFrame = extractRDBMSData(config.)
    //      loadDataToS3(dataFrame,config.NUM_PARTITIONS, config.MODE_TYPE, config.PARTITION_COLUMNS,config.TARGET_OUTPUT)
    //    }
    else if (source == "ES" && target == "S3") {
      val dataFrame: DataFrame = extractESData(config.SOURCE_INPUT, config.INPUT_OPTIONS)
      loadDataToS3(config.INGESTION_COMPLEXITY, dataFrame, config.NUM_PARTITIONS, config.MODE_TYPE, config.PARTITION_COLUMNS, config.OUTPUT_FORMAT, config.TARGET_OUTPUT, config.OUTPUT_OPTIONS)
    }
    stopApp()
  }

}
