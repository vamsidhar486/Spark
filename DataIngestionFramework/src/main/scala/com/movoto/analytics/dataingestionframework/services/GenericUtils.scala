package com.movoto.analytics.dataingestionframework.services

import com.movoto.analytics.dataingestionframework.conf.AppConfig
import com.movoto.analytics.dataingestionframework.ops.SparkOps
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.elasticsearch.spark.sql._


trait GenericUtils extends AppConfig{

  lazy val spark:SparkSession = SparkOps.getSparkSession(config.APP_NAME)

  def getSparkConf: SparkConf = {
    spark.sparkContext.getConf
  }

  def getAppID: String = {
    spark.sparkContext.applicationId
  }

  def getAppName: String = {
    spark.sparkContext.appName
  }

  def exitApp: Nothing = {
    sys.exit(1)
  }

  def stopApp(): Unit = {
    spark.stop()
  }

  def extractHiveData(statement: String): DataFrame ={
    spark.sql(statement)
  }

  def extractS3Data(inputPath: String, dataFormat: String): DataFrame = {
    spark.read.format(dataFormat).load(inputPath)
  }

  def extractHDFSData(inputPath: String, dataFormat: String): DataFrame = {
    spark.read.format(dataFormat).load(inputPath)
  }

  def extractRDBMSData(driver: String, url: String, statement: String, user: String, password: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", statement)
      .option("user", user)
      .option("password",password)
      .load()
  }

  def extractESData(esIndex: String, options: Map[String,String]): DataFrame = {
    spark.read.format("org.elasticsearch.spark.sql")
      .options(options)
      .load(esIndex)
  }

  def loadDataToHDFS(ingestionType: String = "SMALL", dataFrame: DataFrame, numPartitions: Int = 1, modeType: String = "append", partitionColumns: List[String] = List.empty[String], outputFormat: String = "parquet", outputPath: String): Unit = {
    logger.info(s"Loading $dataFrame data in to HDFS path $outputPath in $modeType mode with $numPartitions partitions based on $partitionColumns")
    val columns = partitionColumns.mkString(",").split(",").map(x => col(x))
    if(partitionColumns.nonEmpty && ingestionType == "MEDIUM" ) {
      dataFrame
        .repartition(columns: _*)
        .write
        .format(outputFormat)
        .mode(modeType)
        .partitionBy(partitionColumns: _*)
        .save(outputPath)
    }
    else if(partitionColumns.nonEmpty && ingestionType == "LARGE") {
      dataFrame
        .coalesce(numPartitions)
        .write
        .format(outputFormat)
        .mode(modeType)
        .partitionBy(partitionColumns: _*)
        .save(outputPath)
    }
    else {
        dataFrame
          .repartition(numPartitions)
          .write
          .format(outputFormat)
          .mode(modeType)
          .save(outputPath)
    }
    logger.info(s"Loaded $dataFrame data in to HDFS $outputPath in $modeType mode with $numPartitions partitions based on $partitionColumns")
  }

  def loadDataToHive(ingestionType: String, dataFrame: DataFrame, numPartitions: Int = 1,modeType: String = "append", partitionColumns: List[String] = List.empty[String], outputFormat: String = "parquet", table: String): Unit = {
    logger.info(s"Loading $dataFrame data in to hive table $table in $modeType mode with $numPartitions partitions based on $partitionColumns")
    val columns = partitionColumns.mkString(",").split(",").map(x => col(x))
    if(partitionColumns.nonEmpty && ingestionType == "MEDIUM" ) {
      dataFrame
        .repartition(columns: _*)
        .write
        .format(outputFormat)
        .mode(modeType)
        .partitionBy(partitionColumns: _*)
        .insertInto(table)
    }
    else if(partitionColumns.nonEmpty && ingestionType == "LARGE") {
      dataFrame
        .repartition(numPartitions, columns: _*)
        .write
        .format(outputFormat)
        .mode(modeType)
        .partitionBy(partitionColumns: _*)
        .insertInto(table)
    }
    else {
      dataFrame
        .repartition(numPartitions)
        .write
        .format(outputFormat)
        .mode(modeType)
        .insertInto(table)
    }
    logger.info(s"Loaded $dataFrame data in to hive table $table in $modeType mode with $numPartitions partitions based on $partitionColumns")
  }

  def loadDataToS3(ingestionComplexity: String, dataFrame: DataFrame, numPartitions: Int = 1, modeType: String = "append", partitionColumns: List[String] = List.empty[String], outputFormat: String = "parquet", outputPath: String, outputOptions: Map[String,String]): Unit = {
    logger.info(s"Loading $dataFrame data in to HDFS path $outputPath in $modeType mode with $numPartitions partitions based on $partitionColumns")
    val columns = partitionColumns.mkString(",").split(",").map(x => col(x))
    if(partitionColumns.nonEmpty && ingestionComplexity == "MEDIUM" ) {
      dataFrame
        .repartition(columns: _*)
        .write
        .format(outputFormat)
        .options(outputOptions)
        .mode(modeType)
        .partitionBy(partitionColumns: _*)
        .save(outputPath)
    }
    else if(partitionColumns.nonEmpty && ingestionComplexity == "LARGE") {
      dataFrame
        .repartition(numPartitions)
//        .repartition(numPartitions,columns: _*)
        .write
        .format(outputFormat)
        .options(outputOptions)
        .mode(modeType)
        .partitionBy(partitionColumns: _*)
        .save(outputPath)
    }
    else {
      dataFrame
        .repartition(numPartitions)
        .write
        .format(outputFormat)
        .options(outputOptions)
        .mode(modeType)
        .save(outputPath)
    }
    logger.info(s"Loaded $dataFrame data in to HDFS $outputPath in $modeType mode with $numPartitions partitions based on $partitionColumns")
  }

  /**
   * Method to load data to target Elasticsearch bucket
   * @param dataFrame A business dataframe which has to be loaded to target
   * @param indexType Elasticsearch index name with type to which the data has to be pushed
   */
  def loadDataES(dataFrame: DataFrame, indexType:String): Unit = {
    logger.info(s"""Data transformed in to dataframe $dataFrame""")
    dataFrame.saveToEs(indexType)
  }

  def runDataIngestion(source: String, target: String): Unit = {

  }

}
