# HDFS File Optimizer

## Goal

Optimize HDFS files (ORC & Parquet format ONLY) size by reducing big files and merge small files together using a simple Spark application.

Application will scan HDFS directories (and subdirectories recursively) specified by option ``spark.hdfs.dirs``.

If files are larger than ``spark.files.max.size`` (by default 500 MB ), 
file will be split to reach an optimal size defined by ``spark.files.optimal.size`` (by default 128 MB)

If files are smaller than ``spark.files.min.size`` (by default 100 MB ), 
all files with same extension in the same directory will be merged to reach an optimal size defined by ``spark.files.optimal.size`` (by default 128 MB)

Optimized files are written in a directory called as file name with an _optimized append to its name.

## How to launch it ? 

Compile it first using:
        
        mvn clean package

Run it using the spark-submit shell provided and get the jar with dependencies created by above command:

    spark-submit \
    --class com.cloudera.frisch.hdfs_file_optimizer.App \
    --files log4j.properties \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
    --conf "spark.hdfs.dirs=/tmp/random_datagen/hdfs/employee_csv_big/" \
    hdfs_file_optimizer.jar -Dconfig.file=application.conf 

Note: To test it, a shell script [data_generator_test.sh](src/main/resources/data_generator_test.sh) can help to generate different types and size of data.

Note: To help to deploy it, a script [copy_to_cluster.sh](copy_to_cluster.sh) can help.

## Important parameters

Below parameters can be set using ```--conf "parameterName=Value"``` in spark-submit command.

spark.hdfs.dirs : A comma separated list of directories to scan (it will be recursive so one can specify a common root directory)
spark.files.min.size : Minimum file size, files lower than this will be merged if possible
spark.files.max.size : Maximum file size, files greater than this will be split if possible
spark.files.optimal.size : Optimal file size, file size to try to reach


All others parameters for Spark can be specified in the spark-submit command, especially keytab and principal if running on kerberized environment 
and fine tuning parameters on memory and cpu (required for large files scanning or great number of small files).


## Limitations

- Due to schema inference, CSV and JSON are limited and not really working.
- Avro and Text files not implemented
- Already optimized directories will not be treated