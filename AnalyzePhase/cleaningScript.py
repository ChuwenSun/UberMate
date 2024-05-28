from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import LongType, DoubleType, TimestampType
from functools import reduce
from pyspark.sql.functions import dayofweek, hour, minute
from pyspark import SparkContext
from datetime import datetime
import time
import json
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("Preprocess Parquet Files").enableHiveSupport().getOrCreate()

# function: read into the current status file to look for permission to start(TODO)
# if the file is saying the Analysis process is finished, this function will return true
def checkForPermission(permissionFile):
    with open(permissionFile, 'r') as file:
        content = file.read().strip()
    return content == "kmean finsished"  

# function: modify the permission file for the Analysis process to start for ready cleaned data
# Note: the file will be modified with the current time stamp so it is showing unique information
# of the time of current cleaned data
def givePermission():
    with open(givePermissionFile, 'w') as file:
        file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# function: return hour_of_week with the datatime_column of the data
# Note: split the whole week into 168 hours and calculate how many hours it is since last Sunday midnight
def hour_of_week(datetime_col):
    # Day of week returns 1 for Sunday, 2 for Monday, etc.
    # Adjust the day of week to start from 0 (Monday)
    adjusted_dayofweek = ((dayofweek(datetime_col) + 5) % 7)
    hour_of_week = (adjusted_dayofweek * 24) + hour(datetime_col)
    minute_fraction = minute(datetime_col) / 60.0
    return hour_of_week + minute_fraction


# function: to list files in an HDFS directory
def list_files_in_hdfs_with_size(spark, directory):
    hadoop = spark._jvm.org.apache.hadoop
    conf = spark._jsc.hadoopConfiguration()
    fs = hadoop.fs.FileSystem.get(conf)
    path = hadoop.fs.Path(directory)

    return [(str(f.getPath()), fs.getFileStatus(f.getPath()).getLen() / 1048576) for f in fs.listStatus(path)]
    # hadoop = spark._jvm.org.apache.hadoop
    # fs = hadoop.fs.FileSystem
    # conf = hadoop.conf.Configuration()
    # path = hadoop.fs.Path(directory)
    # return [(str(f.getPath()), fs.getFileStatus(f.getPath()).getLen() / 1048576) for f in fs.listStatus(path)]

# function: cast columns to desired types
def cast_column(df, column_name, target_type):
    for column in columnDict[column_name]:
        if column in df.columns:
            return df.withColumn(column_name, df[column].cast(target_type))
    return df.withColumn(column_name, lit(None).cast(target_type))

# function: load already processed files and return that as a set of files
def load_processed_files(local_dir, filename):
    full_path = local_dir + '/' + filename
    try:
        with open(full_path, 'r') as file:
            return set(json.load(file))
    except FileNotFoundError:
        return set()

# function: save already processed files and update the file that is keeping info about which files are processed
def save_processed_files(local_dir, filename, processed_files):
    full_path = local_dir + '/' + filename
    with open(full_path, 'w') as file:
        json.dump(list(processed_files), file)

# Column name mapping
columnDict = {
    "PULocationID": ["PULocationID"],
    "DOLocationID": ["DOLocationID"],
    "base_passenger_fare": ["base_passenger_fare", "fare_amount"],
    "pickup_datetime": ["pickup_datetime", "lpep_pickup_datetime", "tpep_pickup_datetime"],
    "dropoff_datetime": ["dropoff_datetime", "lpep_dropoff_datetime", "tpep_dropoff_datetime"]
}

# Directory where your parquet files are stored
input_directory = "hdfs://localhost:9000/raw"

# Directory where you want to save the preprocessed files
# output_root_directory = "hdfs://localhost:9000/cleaned"

local_dir = "/home/ubermate/json"
processed_files_filename = "raw_processed.json"
givePermissionFile = "/home/ubermate/json/newData_signal.txt"
permissionFilePath = "/home/ubermate/json/kmeanPermission.txt"

sleepCounter = 0
batch_counter = 0


while True:
    # First: get set of new files that needed to be be cleaned up by (all_files - already_processed_files)

    # List all files in the directory
    file_info = list_files_in_hdfs_with_size(spark, input_directory)
    already_processed = load_processed_files(local_dir, processed_files_filename)

    new_files = [(path, size) for path, size in file_info if path not in already_processed and path.endswith('.parquet') and not os.path.basename(path).startswith('.')]

    # First: get set of new files that needed to be be cleaned up by (all_files - already_processed_files)

    # List all files in the directory
    if new_files:
        sleepCounter = 0
    
    # Process each file and collect DataFrames
    dataframes = []
    batch_size = 0
    for file_path, file_size_mb in new_files:
        batch_size += file_size_mb
        # start to generate this batch of cleaned data when sum of the size of raw data is greater than 600MB
        # Start ingesting a batch of cleaned data into Hive DB
        # when raw data files has built up to a desinated batch maximum size
        if batch_size >= 1000:
            for i in range(10):
                print("current file name: " + file_path + "size: " + str(file_size_mb))
                print("A Batch starting to ingest into Hive")
            break
        # Only taking the columns that we needed into the data frame
        # Note: raw data files has different format, the format difference issue is addressed in columnDict
        df = spark.read.parquet(file_path)
        for column in columnDict:
            df = cast_column(df, column, LongType() if column in ["PULocationID", "DOLocationID"] else DoubleType() if column == "base_passenger_fare" else TimestampType())
        
        # Data cleaning
        # Drop rows where any of the specified columns is NULL
        df = df.dropna(how='any', subset=["PULocationID", "DOLocationID", "base_passenger_fare", "pickup_datetime", "dropoff_datetime"])
        df = df.withColumn("pickup_hour_of_week", hour_of_week(col("pickup_datetime")))
        df = df.withColumn("dropoff_hour_of_week", hour_of_week(col("dropoff_datetime")))

        dataframes.append(df.select("PULocationID", "DOLocationID", "base_passenger_fare", "pickup_datetime", "dropoff_datetime", "pickup_hour_of_week", "dropoff_hour_of_week"))
        already_processed.add(file_path)
    
    # Union all the DataFrames and start to ingest the dataframes into Hive DB
    if dataframes:
        batch_counter += 1
        final_df = dataframes[0]
        for df in dataframes[1:]:
            final_df = final_df.union(df)

        # Write the processed data back to parquet files
        # final_df.write.mode("overwrite").parquet(output_root_directory + '/' + datetime.now().strftime("%Y%m%d%H%M%S"))
        hive_table = "cleaned_table"
        final_df.createOrReplaceTempView("temp_table")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {hive_table} AS SELECT * FROM temp_table")
        # final_df.write.mode("append").saveAsTable("old_table")
        final_df.write.mode("append").insertInto(hive_table)

        # if any new data was added into Hive, 
        # signal file will be updated with current date and time to signal Analysis script to run


        save_processed_files(local_dir, processed_files_filename, already_processed)
        for i in range(10):
            print("processed file Set saved to " + local_dir + '/' + processed_files_filename)
            print("json file updated between batch")
    #Script pauses and wait when no more new_files, which means all files are processed in this dir
    # There will be two situation that we starting the Analysis script:
    # 1. when we have 10 batches of data cleaned
    # 2. when we have less than 10 batches of data cleaned, but there are no new data coming in for 2 min
    if not new_files:
        for i in range(10):
            print("No more new files to process")
        save_processed_files(local_dir, processed_files_filename, already_processed)
        for i in range(10):
            print("processed file Set saved to " + local_dir + '/' + processed_files_filename)
        for i in range(10):
            print("waiting for new files, SLEEPING FOR 10 SECS......  TOTAL SLEEP TIME: " + str(sleepCounter))
        time.sleep(10)
        sleepCounter += 10
        if sleepCounter >= 60:
            if batch_counter != 0:
                for i in range(10):
                    print("NO NEW FILES FOR 2 MINS and less than 10 GB of new data are ready in Hive")
                #change batch counter to 10 so script will go to sleep and make algorithm part run
                batch_counter = 10
            else:
                for i in range(10):
                    print("NO NEW FILES FOR 2 MINS, there are no new data ready in Hive")
                for i in range(10):
                    print("Waiting for new Data to clean......")
    if batch_counter >= 10:
        for i in range(10):
            print("Permission to Hive giving to algorithms......")
        with open(permissionFilePath, 'w') as file:
            file.write("kmean running now...")
        givePermission()
        spark.stop()
        while True:
            if not checkForPermission(permissionFilePath):
                for i in range(10):
                    print("No permission to Hive yet, Sleeping to wait for permission from Algorithm part...")
                time.sleep(10)
            else:
                spark = SparkSession.builder.appName("Preprocess Parquet Files").enableHiveSupport().getOrCreate()
                batch_counter = 0
                sleepCounter = 0
                break

# Stop the Spark Session
spark.stop()
