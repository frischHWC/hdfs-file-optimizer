#!/usr/bin/env bash
export HOST=ccycloud-1.snam.root.hwx.site
export USER=root
export DIRECTORY_TO_WORK=hdfs_file_optimizer/

# Create directory folder on cluster
ssh ${USER}@${HOST} mkdir -p ${DIRECTORY_TO_WORK}

# Copy files to cluster
scp src/main/resources/data_generator_test.sh ${USER}@${HOST}:~/${DIRECTORY_TO_WORK}
ssh ${USER}@${HOST} chmod 775 ${DIRECTORY_TO_WORK}data_generator_test.sh
scp spark-submit.sh ${USER}@${HOST}:~/${DIRECTORY_TO_WORK}
ssh ${USER}@${HOST} chmod 775 ${DIRECTORY_TO_WORK}spark-submit.sh
scp src/main/resources/application.conf ${USER}@${HOST}:~/${DIRECTORY_TO_WORK} 
scp src/main/resources/log4j.properties ${USER}@${HOST}:~/${DIRECTORY_TO_WORK}
scp README.MD ${USER}@${HOST}:~/${DIRECTORY_TO_WORK}
scp target/hdfs_file_optimizer-0.1-SNAPSHOT-jar-with-dependencies.jar ${USER}@${HOST}:~/${DIRECTORY_TO_WORK}/hdfs_file_optimizer.jar

# Check everything is in target directory
ssh ${USER}@${HOST} ls -ali ${DIRECTORY_TO_WORK}

# Generates data on the platform
ssh ${USER}@${HOST} 'sh hdfs_file_optimizer/data_generator_test.sh'

# Launch it on platform
#ssh ${USER}@${HOST} 'sh hdfs_file_optimizer/spark-submit.sh'