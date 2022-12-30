from pyspark.sql import SparkSession

import pandas as pd

def build_spark_session():
    spark = (SparkSession
             .builder
             .appName("Python Spark SQL basic example")
             # .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             # .config(
             #     "spark.driver.extraJavaOptions",
             #     "-Dio.netty.tryReflectionSetAccessible=true",
             # )
             # .config(
             #     "spark.executor.extraJavaOptions",
             #     "-Dio.netty.tryReflectionSetAccessible=true",
             # )
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
             .config("spark.driver.memory", "4g")
             .getOrCreate())
    return spark

def execute_query(spark, query: str):
    df = spark.read.format("parquet").load("s3a://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2022-40/subset=warc/*.parquet")
    df.createOrReplaceTempView("cc_index")
    df = spark.sql(query)
    return df

def process_warcs(df):
    rows = df.select(F.concat(F.lit("s3a://commoncrawl/"), F.col("warc_filename")))  # crawl-data/CC-MAIN-2022-40/segments/"), F.col("warc_segment"), "/warc/", F.col("warc_filename"))).collect()
    paths = [row[0] for row in rows]
    
if __name__ == "__main__":
    spark = build_spark_session()

    # query = """
    # SELECT url, content_mime_detected, warc_filename, warc_record_offset, warc_record_length, warc_segment
    # FROM cc_index
    # WHERE content_mime_detected = 'text/vtt'
    #    OR endswith(url, '.vtt')
    #    OR endswith(url, '.srt')"""
    # df = execute_query(spark, query)
    # df.write.parquet("subtitles_full.parquet")


    #    , content_mime_detected, warc_filename, warc_record_offset, warc_record_length, warc_segment
    query = """
    SELECT url
    FROM cc_index
    WHERE contains(url, 'creativecommons.org/licenses')
    """
    df = execute_query(spark, query)
    df.explain(mode="codegen")
    df.write.mode("overwrite").json("licenses.jsonl")

    # regex = r"^http[s]?://(?:www[.]|)creativecommons[.]org/licenses/(?:publicdomain|by|by-sa)"
    regex = "^http[s]?:\/\/(?:www[.]|)creativecommons[.]org\/licenses\/(?:publicdomain|by|by-sa).*"
    # by/3.0/
    
    # pdf = pd.read_parquet("subtitles_full.parquet")
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', None):
    #     print(pdf.head(10))

    # https://creativecommons.org/licenses/publicdomain/
    # 
 
