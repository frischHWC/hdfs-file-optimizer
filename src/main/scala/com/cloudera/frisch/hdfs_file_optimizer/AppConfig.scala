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
