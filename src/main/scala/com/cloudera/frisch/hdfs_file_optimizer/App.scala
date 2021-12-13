package com.cloudera.frisch.hdfs_file_optimizer


import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object App {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  /**
   * Main function that creates a SparkContext and launches treatment
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    logger.info(" Starting Spark Program ")

    // Create Spark Context
    val spark = SparkSession
      .builder()
      .appName(AppConfig.name)
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()

    // Retrieves parameters from command line and set them
    AppConfig.hdfsScanDirs = spark.conf.get("spark.hdfs.dirs", "/tmp/").split(",")
    AppConfig.minSize = spark.conf.get("spark.files.min.size", "100").toInt * 1024 * 1024
    AppConfig.maxSize = spark.conf.get("spark.files.max.size", "500").toInt * 1024 * 1024
    AppConfig.optimalSize = spark.conf.get("spark.files.optimal.size", "128").toInt * 1024 * 1024

    // Launch treatment
    Treatment.treatment(spark)

    // Stop Spark Context
    spark.stop()

    logger.info(" Stopped Spark Program ")
  }

}
