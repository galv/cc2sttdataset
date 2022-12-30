from cc2dataset.video_transcript_finder import process_one_part, process_multi_part, get_warc_files_offsets_with_transcript_and_video
from cc2dataset.main import read_wat_index_files
from pyspark.sql import SparkSession

import fsspec

def build_spark_session():
    spark = (SparkSession
             .builder
             .appName("Python Spark SQL basic example")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config(
                 "spark.driver.extraJavaOptions",
                 "-Dio.netty.tryReflectionSetAccessible=true",
             )
             .config(
                 "spark.executor.extraJavaOptions",
                 "-Dio.netty.tryReflectionSetAccessible=true",
             )
             .config("spark.driver.memory", "32g")
             # .config("spark.python.profile", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.DefaultAWSCredentialsProviderChain')
             .getOrCreate())
    return spark

if __name__ == "__main__":
    local_wat_paths = [
        "/home/ext_dt_galvez_gmail_com/code/cc2dataset/examples/local_wats/CC-MAIN-20210505203909-20210505233909-00000.warc.wat.gz",
        "/home/ext_dt_galvez_gmail_com/code/cc2dataset/examples/local_wats/CC-MAIN-20210505203909-20210505233909-00001.warc.wat.gz",
        "/home/ext_dt_galvez_gmail_com/code/cc2dataset/examples/local_wats/CC-MAIN-20210505203909-20210505233909-00002.warc.wat.gz",
        "/home/ext_dt_galvez_gmail_com/code/cc2dataset/examples/local_wats/CC-MAIN-20210505203909-20210505233909-00003.warc.wat.gz",
        "/home/ext_dt_galvez_gmail_com/code/cc2dataset/examples/local_wats/CC-MAIN-20210505203909-20210505233909-00004.warc.wat.gz",
    ]
    wat_paths = [
        "s3://commoncrawl/crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00000.warc.wat.gz",
        "s3://commoncrawl/crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00001.warc.wat.gz",
        "s3://commoncrawl/crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00002.warc.wat.gz",
        "s3://commoncrawl/crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00003.warc.wat.gz",
        "s3://commoncrawl/crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00004.warc.wat.gz",
        # "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00005.warc.wat.gz",
        # "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00006.warc.wat.gz",
        # "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00007.warc.wat.gz",
        # "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00008.warc.wat.gz",
        # "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00009.warc.wat.gz",
    ]

    for wat_path in wat_paths:
        results = list(get_warc_files_offsets_with_transcript_and_video(wat_path))
        print(len(results))
