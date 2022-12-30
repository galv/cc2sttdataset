from cc2dataset.video_transcript_finder import process_one_part, process_multi_part
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
    wat_paths = [
        "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00000.warc.wat.gz",
        "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00001.warc.wat.gz",
        "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00002.warc.wat.gz",
        "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00003.warc.wat.gz",
        "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00004.warc.wat.gz",
        "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00005.warc.wat.gz",
        "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00006.warc.wat.gz",
        "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00007.warc.wat.gz",
        "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00008.warc.wat.gz",
        "crawl-data/CC-MAIN-2021-21/segments/1620243988696.23/wat/CC-MAIN-20210505203909-20210505233909-00009.warc.wat.gz",
    ]

    output_path = "videos_and_transcripts_large_16_cores"
    resume = True
    if not resume:
        wat_index_files = read_wat_index_files(None, None, "s3")
        # write wat index files to disk in output_path with fsspec
        with fsspec.open(f"{output_path}/wat_index_files.txt", "w", encoding="utf8") as f:
            f.write("\n".join(wat_index_files))
    else:
        with fsspec.open(f"{output_path}/wat_index_files.txt", "r", encoding="utf8") as f:
            wat_index_files = f.read().splitlines()


    # was 1_000_000 at first
    process_multi_part(output_path, wat_index_files, build_spark_session, 10_000, False, resume, "s3")

    # wat_index_files = wat_index_files[:100]
    # process_one_part("videos_and_transcripts_100.jsonl", wat_index_files, build_spark_session, True, "s3")
