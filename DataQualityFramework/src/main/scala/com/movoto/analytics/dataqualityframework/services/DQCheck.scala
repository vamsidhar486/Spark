package com.movoto.analytics.dataqualityframework.services

// The DQ check Class
class DQCheck {
  /**
   * A method to call data quality check
   * @param checkType
   */
  def check(checkType: String): Unit = {
    if (checkType == "NULL_CHECK") {
      NullCheck()
    }
    if (checkType == "COUNT_CHECK") {
      val countCheck = new CountCheck()
      countCheck.countCheck()
    }
    if (checkType == "DISTINCT_COUNT_CHECK") {
      DistinctCountCheck()
    }
    if (checkType == "AVERAGE_CHECK") {
      AverageCheck()
    }
    if (checkType == "MINIMUM_CHECK") {
      MinimumCheck()
    }
    if (checkType == "MAXIMUM_CHECK") {
      MaximumCheck()
    }
    if (checkType == "SUM_CHECK") {
      SumCheck()
    }
    if (checkType == "QUALITY_CHECK") {
      QualityCheck()
    }
    if (checkType == "AGGREGATIONS_CHECK") {
      AggregationsCheck()
    }
  }
}
