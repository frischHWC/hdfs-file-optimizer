package com.cloudera.frisch.hdfs_file_optimizer

import com.cloudera.frisch.hdfs_file_optimizer.Treatment.FileType.FileType
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


object Treatment {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)


  object FileType extends Enumeration {
    type FileType = Value
    val JSON, ORC, CSV, PARQUET = Value
  }

  /**
   * Spark treatment
   */
  def treatment(spark: SparkSession): Unit = {

    val hdfsClient = FileSystem.newInstance(spark.sparkContext.hadoopConfiguration)

    AppConfig.hdfsScanDirs.toList.foreach { dir =>
      treatDirectory(spark, hdfsClient, dir)
    }

  }

  def treatDirectory(spark: SparkSession, hdfsClient: FileSystem, path: String): Unit = {
    logger.info("Starting to treat directory : " + path)

    val subDirectories = ListBuffer[String]()
    val csvFilesToTreat = ListBuffer[FileStatus]()
    val orcFilesToTreat = ListBuffer[FileStatus]()
    val jsonFilesToTreat = ListBuffer[FileStatus]()
    val parquetFilesToTreat = ListBuffer[FileStatus]()

    val iterator = hdfsClient.listStatus(new Path(path)).iterator

    // To avoid recursive lock on re-optimizing what's been already optimized, directories already optimized won't be scan and used
    if(path.endsWith("_optimized")) {
      logger.info("This directory has already been optimized previously: " + path + " , so it will not be scanned")
    } else {
      while (iterator.hasNext) {
        val item = iterator.next()
        if (item.isFile) {
          item.getPath.getName match {
            case name if name.endsWith("json") => jsonFilesToTreat += item
            case name if name.endsWith("csv") => csvFilesToTreat += item
            case name if name.endsWith("orc") => orcFilesToTreat += item
            case name if name.endsWith("parquet") => parquetFilesToTreat += item
            case _ => logger.info("File is not recognized so it will not be treated : " + item)
          }
          logger.info("Found file : " + item.getPath.toString + " to treat under : " + path)
        } else {
          logger.info("Path : " + item.getPath.toString + " is a directory that will be scanned and treated later")
          subDirectories += item.getPath.toString
        }
      }

      if (jsonFilesToTreat.nonEmpty) treatFiles(spark, hdfsClient, jsonFilesToTreat, FileType.JSON)
      if (csvFilesToTreat.nonEmpty) treatFiles(spark, hdfsClient, csvFilesToTreat, FileType.CSV)
      if (orcFilesToTreat.nonEmpty) treatFiles(spark, hdfsClient, orcFilesToTreat, FileType.ORC)
      if (parquetFilesToTreat.nonEmpty) treatFiles(spark, hdfsClient, parquetFilesToTreat, FileType.PARQUET)

      if (subDirectories.nonEmpty) subDirectories.foreach(sd => treatDirectory(spark, hdfsClient, sd))

      logger.info("Finished to treat directory : " + path)

    }

  }

  def treatFiles(spark: SparkSession, hdfsClient: FileSystem, path: ListBuffer[FileStatus], fileType: FileType): Unit = {
      // From now on, we consider that files are ok and existing and all of same type and contain same data so they will be merged or split
      val filesToMerge = ListBuffer[FileStatus]()
      val filesToSplit = ListBuffer[FileStatus]()

    path.foreach(f => {
      if(f.getLen > AppConfig.maxSize) {
        filesToSplit += f
        logger.info("File: " + f.getPath.toString + " will be split ")
      } else if (f.getLen < AppConfig.minSize) {
        filesToMerge += f
        logger.info("File: " + f.getPath.toString + " will be merged ")
      }
    })

    if(filesToSplit.nonEmpty) splitFiles(spark, hdfsClient, filesToSplit, fileType)
    if(filesToMerge.nonEmpty) mergeFiles(spark, hdfsClient, filesToMerge, fileType)

  }

  def splitFiles(spark: SparkSession, hdfsClient: FileSystem, files: ListBuffer[FileStatus], fileType: FileType): Unit = {
    files.foreach(file => {
      val partitionsToMake = if(file.getLen / AppConfig.optimalSize > 0) file.getLen / AppConfig.optimalSize else 1
      val outputPath = if (file.getPath.getParent.toString != null)  file.getPath.getParent.toString + "/" else "/"
      val fileNameExtensionStart = file.getPath.getName.lastIndexOf(".")
      val outputfileName = if (fileNameExtensionStart != -1) file.getPath.getName.substring(0, fileNameExtensionStart) + "_optimized" else file.getPath.getName + "_optimized"

      logger.info(" Will split file: " + file.getPath.toString + " into " + partitionsToMake + " files in : " + outputPath )

      var df =
        fileType match {
          case FileType.JSON => spark.read.json(file.getPath.toString)
          case FileType.CSV => spark.read.option("inferSchema", true).csv(file.getPath.toString)
          case FileType.ORC => spark.read.orc(file.getPath.toString)
          case FileType.PARQUET => spark.read.parquet(file.getPath.toString)
        }

      df = df.repartition(partitionsToMake.toInt)

      fileType match {
        case FileType.JSON => df.write.json(outputPath + outputfileName)
        case FileType.CSV => df.write.csv(outputPath + outputfileName)
        case FileType.ORC => df.write.orc(outputPath + outputfileName)
        case FileType.PARQUET => df.write.parquet(outputPath + outputfileName)
      }

      logger.info("Deleting old file as it has been optimized ")
      hdfsClient.delete(file.getPath, false)

      logger.info("Finished to split file: " + file.getPath.toString + " into " + partitionsToMake + " files ")
    })

  }

  def mergeFiles(spark: SparkSession, hdfsClient: FileSystem, files: ListBuffer[FileStatus], fileType: FileType): Unit = {

    if(files.size==1) {
      logger.info("Only one file found to merge: " + files.get(0) + ", so it will not be treated")
    } else {
      var size = 0L
      files.foreach(file => size = size + file.getLen)
      val partitionsToMake = if (size / AppConfig.optimalSize > 0) size / AppConfig.optimalSize else 1
      val outputPath = if (files.get(0).getPath.getParent.toString != null) files.get(0).getPath.getParent.toString + "/" else "/"
      val fileNameExtensionStart = files.get(0).getPath.getName.lastIndexOf(".")
      val outputfileName = if (fileNameExtensionStart != -1) files.get(0).getPath.getName.substring(0, fileNameExtensionStart) + "_optimized" else files.get(0).getPath.getName + "_optimized"
      val pathsList = files.map(f => f.getPath.toString).toList

      logger.info(" Will merge files in: " + pathsList + " into " + partitionsToMake + " files in : " + outputPath)

      var df =
        fileType match {
          case FileType.JSON => spark.read.json(pathsList: _*)
          case FileType.CSV => spark.read.csv(pathsList: _*)
          case FileType.ORC => spark.read.orc(pathsList: _*)
          case FileType.PARQUET => spark.read.parquet(pathsList: _*)
        }

      df = df.coalesce(partitionsToMake.toInt)

      fileType match {
        case FileType.JSON => df.write.json(outputPath + outputfileName)
        case FileType.CSV => df.write.csv(outputPath + outputfileName)
        case FileType.ORC => df.write.orc(outputPath + outputfileName)
        case FileType.PARQUET => df.write.parquet(outputPath + outputfileName)
      }

      logger.info(" Deleting old files as they have been optimized ")
      files.foreach(file => hdfsClient.delete(file.getPath, false))

      logger.info("Finished to merge " + files.size + " files into " + partitionsToMake + " files ")
    }

  }

}
