////package com.movoto.analytics.dataingestionframework.services
//
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
//
//class SDVCheck() extends GenericUtils {
//
//  var recordsWrittenCount: Long = 0
//  spark.addSparkListener(new SparkListener() {
//    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
//      synchronized {
//        recordsWrittenCount += taskEnd.taskMetrics.outputMetrics.recordsWritten
//      }
//    }
//  })
//
//  def SDVCheck(sourceDataframe: DataFrame, writtenRecords:Long): Unit = {
//    if(sourceDataframe.count() == writtenRecords) {
//      logger.info("SDV check passed.")
//    } else {
//      logger.info("SDV check failed.")
//    }
//  }
//
//}
