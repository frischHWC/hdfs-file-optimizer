#!/usr/bin/env bash

spark-submit \
    --class com.cloudera.frisch.hdfs_file_optimizer.App \
    --master yarn \
    --deploy-mode cluster \
    --files log4j.properties \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
    --principal dev \
    --keytab /home/dev/dev.keytab \
    --conf spark.extraListeners='' \
    --conf spark.sql.queryExecutionListeners='' \
    --conf spark.sql.streaming.streamingQueryListeners='' \
    --conf "spark.hdfs.dirs=/tmp/random_datagen/hdfs/employee_orc_big/,/tmp/random_datagen/hdfs/employee_orc_small/" \
    --conf "spark.files.max.size=300" \
    hdfs_file_optimizer.jar -Dconfig.file=application.conf 
    