package com.movoto.analytics.dataingestionframework.services

import com.movoto.analytics.dataingestionframework.conf.AppConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, countDistinct}

trait DataQualityCheck extends AppConfig {

  def nullCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, columnName: String) = {
    sourceDataFrame.filter(sourceDataFrame[columnName].isNull()).limit(1)

  }



  def countCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, columnName: Option[String]) = {
    if(sourceDataFrame.count() == targetDataFrame.count()){
      logger.info("Count is matching")
    }
    else {
      logger.warn("WARN: Count is not matching. Please check")
    }
  }

  def distinctCount(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, columnName: String)= {
    if(sourceDataFrame.countDistinct == sourceDataFrame.countDistinct()) {
      logger.info("INFO: Count is matching")
    }
    else {
      logger.warn("WARN: Distinct Count is not matching. Please check")
    }
  }

  def avgCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, groupColumnNames: String, aggColumnName: String ) ={
    if(sourceDataFrame.groupBy(groupColumnNames).avg(aggColumnName) == targetDataFrame.groupBy(groupColumnNames).avg(aggColumnName)) {
      logger.info("INFO: Average is matching")
    }
    else {
      logger.warn("WARN: Average is not matching")
    }
  }

  def maxCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, groupColumnNames: String, aggColumnName:String) ={
    if(sourceDataFrame.groupBy(groupColumnNames).max(aggColumnName) == targetDataFrame.groupBy(groupColumnNames).max(aggColumnName)) {
      logger.info("INFO: Max Check is passed")
    }
    else {
      logger.warn("WARN: Max Check is failed")
    }
  }

  def minCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, groupColumnNames: String, aggColumnName: String) ={
    if(sourceDataFrame.groupBy(groupColumnNames).min(aggColumnName) == targetDataFrame.groupBy(groupColumnNames).min(aggColumnName)) {
      logger.info("INFO: MIN check is passed")
    }
    else {
      logger.warn("WARN: MIN Check is failed")
    }
  }

  def sumCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, groupColumnNames: String, aggColumnName:String) ={
    if(sourceDataFrame.groupBy(groupColumnNames).sum(aggColumnName) == targetDataFrame.groupBy(groupColumnNames).sum(aggColumnName)) {
      logger.info("INFO: SUM Check is passed")
    }
    else {
      logger.warn("WARN: SUM check is failed")
    }
  }

  def percentileCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame, groupColumnNames: String, aggColumnName:String) ={
    if(sourceDataFrame.groupBy(groupColumnNames).sum(aggColumnName) == targetDataFrame.groupBy(groupColumnNames).sum(aggColumnName)) {
      logger.info("INFO: SUM Check is passed")
    }
    else {
      logger.warn("WARN: SUM check is failed")
    }
  }

  def DQCheck(rule: String): Unit = {
      row = row


  }

}
