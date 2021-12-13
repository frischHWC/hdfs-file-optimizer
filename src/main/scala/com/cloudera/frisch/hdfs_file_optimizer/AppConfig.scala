package com.cloudera.frisch.hdfs_file_optimizer

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {

  val conf: Config = ConfigFactory.load()

  val name: String = conf.getString("appName")
  val master: String = conf.getString("master")
  val debug: Boolean = conf.getBoolean("debug")

  var hdfsScanDirs: Array[String] = Array("/tmp/")
  var minSize: Int = 100 * 1024 * 1024
  var maxSize: Int = 500 * 1024 * 1024
  var optimalSize: Int = 128 * 1024 * 1024


}
