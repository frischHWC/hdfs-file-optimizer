/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
