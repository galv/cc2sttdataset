from fastwarc.warc import ArchiveIterator, WarcRecord, WarcRecordType, is_http
from fastwarc.stream_io import GZipStream, BytesIOStream
# from fastwarc.stream_io import GZipStream
import fsspec
import simdjson
from io import BytesIO
import cgi
import math

import os

import pandas as pd

from html.parser import HTMLParser

from typing import Dict, Tuple, List

from pyspark.sql import types as T
from pyspark import SparkContext

from cc2dataset.main import may_be_creative_commons, extract_documents_from_links, get_last_successful_part, valid_video_link, valid_transcript_link

import time

# import logging
# import s3fs
# logging.basicConfig()  # not sure if you need this
# s3fs.core.logger.setLevel("DEBUG")

# We recommend that you run your computing workload in the same region (us-east-1) as the Common Crawl dataset whenever possible. 

def get_warc_files_offsets_with_transcript_and_video(wat_file_path):
    with fsspec.open(wat_file_path, "rb") as fh:
        for i in range(10):
            try:
                tf = BytesIO(fh.read())
                # tf = GZipStream(BytesIOStream(fh.read()))
                break
            except Exception as ex:  # pylint: disable=broad-except
                if i == 9:
                    print("failed 10 times, skipping ", wat_file_path)
                    return
                print(ex)
                print(f"retrying reading {i}/10")
                time.sleep(1)

    parser = simdjson.Parser()

    #
    for record in ArchiveIterator(tf, record_types=WarcRecordType.metadata, parse_http=False):
        record_data, envelope, links, http_response_metadata, metadata = None, None, None, None, None
        del record_data, envelope, links, http_response_metadata, metadata
        record_data = parser.parse(record.reader.read())
        try:
            envelope = record_data["Envelope"]
            metadata = envelope["Payload-Metadata"]
            http_response_metadata = metadata["HTTP-Response-Metadata"]
            links = http_response_metadata["HTML-Metadata"]["Links"]
        except:
            continue

        if (not may_be_creative_commons(links)
            or len(extract_documents_from_links(links, "transcript")) == 0
            or len(extract_documents_from_links(links, "video")) == 0):
            continue
        source_uri = envelope["WARC-Header-Metadata"]["WARC-Target-URI"]

        prefix = os.path.dirname(os.path.dirname(wat_file_path))
        path = prefix + "/warc/" + record_data["Container"]["Filename"]
        offset = int(record_data["Container"]["Offset"])
        yield (path, offset, source_uri)

class MyHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.videos: list[list[dict[str,str]]] = []
        self.tracks: list[list[dict[str,str]]] = []
        self.inside_video = False

    def handle_starttag(self, tag, attrs):
        if tag == "video":
            self.inside_video = True
            self.videos.append([])
            self.tracks.append([])
        elif tag == "source" and self.inside_video:
            # These aren't strings!
            self.videos[-1].append(attrs)
        elif tag == "track" and self.inside_video:
            # These aren't strings
            self.tracks[-1].append(attrs)

    def handle_endtag(self, tag):
        if tag == "video":
            self.inside_video = False

    def handle_data(self, data):
        pass


# wat files don't have point to the relevant wet file
# unfortunately. They point only to the warc file. Ugh.

READ_WARC_RETURN_SCHEMA = T.StructType(
    [
        T.StructField("videos", T.ArrayType(T.ArrayType(T.MapType(T.StringType(), T.StringType())))),
        T.StructField("transcripts", T.ArrayType(T.ArrayType(T.MapType(T.StringType(), T.StringType())))),
        T.StructField("source_uri", T.StringType())
    ]
)

def read_warc(file_path_tuple: Tuple[str], pdf: pd.DataFrame) -> pd.DataFrame:
    file_path,  = file_path_tuple
    source_uris = set(pdf.source_uri)
    def contains_offset(record: WarcRecord):
        within = record.headers["WARC-Target-URI"] in source_uris
        if within:
            source_uris.remove(record.headers["WARC-Target-URI"])
        return within
        # return pdf.source_uri.str.contains(record.headers["WARC-Target-URI"], regex=False).any()
        # return True
        # return record.tell() in offset_set
    result_pdf_dict: Dict[str, List[List[List[Dict[str,str]]]]] = {"videos": [], "transcripts": [], "source_uri": []}
    
    with fsspec.open(file_path, "rb") as f:
        assert f.seekable(), "S3 file is not seekable!"
        for offset, source_uri in zip(pdf.offset, pdf.source_uri):
            f.seek(offset)
            iterator = ArchiveIterator(f)
            record = next(iterator)
            assert record.headers["WARC-Target-URI"] == source_uri
            body = record.reader.read()
            parser = MyHTMLParser()
            _, params = cgi.parse_header(record.http_headers["Content-Type"])
            body_s = str(body, encoding=params.get("charset", "utf-8"))
            parser.feed(body_s)
            
            result_pdf_dict["videos"].append(parser.videos)
            result_pdf_dict["transcripts"].append(parser.tracks)
            result_pdf_dict["source_uri"].append(record.headers["WARC-Target-URI"])
    return pd.DataFrame(result_pdf_dict)

        # for record in ArchiveIterator(f, func_filter=contains_offset, record_types=WarcRecordType.response):
        #     # print("GALVEZ:", record.headers)
        #     # print("GALVEZ http:", record.http_headers)
        #     # print("GALVEZ:in= ", record.headers["WARC-Target-URI"] in pdf.source_uri)
        #     # print("GALVEZ: ", record.headers["WARC-Target-URI"])
        #     # if pdf.source_uri.str.contains(record.headers["WARC-Target-URI"]):
        #     body = record.reader.read()
        #     parser = MyHTMLParser()
        #     _, params = cgi.parse_header(record.http_headers["Content-Type"])
        #     body_s = str(body, encoding=params.get("charset", "utf-8"))
        #     parser.feed(body_s)
            
        #     # print("videos=", parser.videos)
        #     # print("tracks=", parser.tracks)
        #     # ('Content-Type', 'text/html; charset=UTF-8')
        #     result_pdf_dict["videos"].append(parser.videos)
        #     result_pdf_dict["transcripts"].append(parser.tracks)
        #     result_pdf_dict["source_uri"].append(record.headers["WARC-Target-URI"])
        #     if len(source_uris) == 0:
        #         break
    # pd.DataFrame.from_records(results, columns=['videos', 'transcripts'])
    # return pd.DataFrame(result_pdf_dict)

def process_one_part(output_path, wat_index_files, build_spark, shuffle, source_cc_protocol):
    """Process one part"""
    spark = build_spark()
    sc = SparkContext.getOrCreate()
    wat_count = len(wat_index_files)
    wat_rdd = sc.parallelize(wat_index_files, wat_count)
    if source_cc_protocol == "s3":
        prefix = "s3://commoncrawl/"
    elif source_cc_protocol == "http":
        prefix = "https://data.commoncrawl.org/"

    def extract(x):
        x = list(x)
        yield from get_warc_files_offsets_with_transcript_and_video(prefix + x[0])

    output = wat_rdd.mapPartitions(extract)
    schema = T.StructType([T.StructField("warc_path", T.StringType()),
                 T.StructField("offset", T.LongType()),
                 T.StructField("source_uri", T.StringType())])
    df = output.toDF(schema)

    # df.write.mode("overwrite").json(output_path)
    # df = spark.read.json(output_path)  # .limit(4)

    df = df.groupBy([df.warc_path]).applyInPandas(read_warc, READ_WARC_RETURN_SCHEMA)
    df.write.mode("overwrite").json(output_path)

    # sc.show_profiles()
    # sc.dump_profiles("profiles_dump")


def process_multi_part(
    output_path, wat_index_files, build_spark, multipart, shuffle, resume, source_cc_protocol
):
    """Process multi part"""
    if resume:
        start_part = get_last_successful_part(output_path) + 1
    else:
        start_part = 0

    wat_count = len(wat_index_files)
    wat_per_part = math.ceil(wat_count / multipart)
    part_paths = []
    for i in range(start_part, multipart):
        start = i * wat_per_part
        end = (i + 1) * wat_per_part
        part_path = f"{output_path}/part_{i}"
        part_paths.append(part_path)
        print(f"Processing part {i} from {start} to {end} into {part_path}")
        process_one_part(part_path, wat_index_files[start:end], build_spark, False, source_cc_protocol)

    # spark = build_spark()
    # print("Merging parts")
    # df = None
    # for part_path in part_paths:
    #     if df is None:
    #         df = spark.read.parquet(part_path)
    #     else:
    #         df = df.union(spark.read.parquet(part_path))

    # deduplicate_repartition_count(df, output_path + "/merged", wat_count, spark, shuffle)
# (env2) ext_dt_galvez_gmail_com@dt-cc2data-aligner:~/code/cc2dataset/examples$ python video_transcript_finder_profiling.py
# Read time= 3.9898481369018555
# Parse time= 20.73528742790222
# 0
# Read time= 3.8896000385284424
# Parse time= 20.30515742301941
# 0
# Read time= 3.6814112663269043
# Parse time= 14.82367730140686
# 0
# Read time= 4.015266180038452
# Parse time= 20.921715021133423
# 0
        
        # print("One iteration")
        # try:
        #     # del record_data
        #     record_data = parser.parse(record.reader.read())
        #     # record_data = simdjson.load(record.reader)  # type: ignore
        # except:  # pylint: disable=bare-except
        #     # raise
        #     print("A shard record failed")
        #     continue
        # # from IPython import embed; embed()
        # print("a")
        # envelope = record_data["Envelope"]
        # try:
        #     source_uri = str(envelope["WARC-Header-Metadata"]["WARC-Target-URI"])
        # except:  # pylint: disable=bare-except
        #     source_uri = "UNKNOWN"
        # else:
        #     pass
        # print("b")
        # payload = envelope["Payload-Metadata"]
        # if "HTTP-Response-Metadata" not in payload:
        #     continue
        # print("c")
        # http_resp = payload["HTTP-Response-Metadata"]
        # if "HTML-Metadata" not in http_resp:
        #     continue
        # print("d")
        # metadata = http_resp["HTML-Metadata"]
        # if "Links" not in metadata:
        #     continue
        # print("e")

        # links = metadata["Links"]

        # if (any(valid_transcript_link(link) for link in links)
        #     and any(valid_video_link(link) for link in links)
        #     and may_be_creative_commons(links)):
        # print("GALVEZ:type(links)", type(links))
        # metadata = record_data["Envelope"]["Payload-Metadata"]
        # if ("HTTP-Response-Metadata" not in metadata
        #     or "HTML-Metadata" not in metadata["HTTP-Response-Metadata"]
        #     or "Links" not in metadata["HTTP-Response-Metadata"]["HTML-Metadata"]):
        #     del record_data
        #     del metadata
        #     continue
        # links = metadata["HTTP-Response-Metadata"]["HTML-Metadata"]["Links"]
        # source_uri = record_data["Envelope"]["WARC-Header-Metadata"]["WARC-Target-URI"]
        # if (not may_be_creative_commons(links)
        #     or len(extract_documents_from_links(links, "transcript")) == 0
        #     or len(extract_documents_from_links(links, "video")) == 0
        #     ):
        #     del record_data
        #     del metadata
        #     del links
        #     continue
