from cc2dataset import cc2dataset
import os

if __name__ == "__main__":
    # if you have a slurm cluster, refer to https://gist.github.com/rom1504/67ada3dedbecc113ae2dbdfd9c642d83 to start a spark cluster there
    cc2dataset(
        "commoncrawl-temp-transcript/",
        wat_index_count=None,
        wat_count=1000,
        master="local",
        num_cores=2,
        mem_gb=6,
        multipart=2,
        document_type="transcript",
        source_cc_protocol="s3",
    )
