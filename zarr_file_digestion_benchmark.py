"""
Benchmark slicing up a large file in S3 into individual zarr files.
"""

from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime
import math
import pathlib
import time
from typing import Dict, List
import uuid

import boto3
from dandischema.digests.dandietag import mb, gb, tb, Part, PartGenerator


MAX_WORKERS = 50
OUTPUT_CSV_HEADERS = [
    "workers",
    "total_time",
    "total_size",
    "avg_part_time",
    "avg_create_upload_time",
    "avg_copy_part_time",
    "avg_complete_upload_time",
]


def kb(bytes_size: int) -> int:
    return bytes_size * 2**10


class DandiPartGenerator(PartGenerator):
    DEFAULT_PART_SIZE = kb(150)
    MAX_PARTS = 100_000_000

    @classmethod
    def for_file_size(cls, file_size: int) -> "PartGenerator":
        """Method to calculate sequential part sizes given a file size"""
        if file_size == 0:
            return cls(0, 0, 0)

        part_size = cls.DEFAULT_PART_SIZE

        if file_size > tb(5):
            raise ValueError("File is larger than the S3 maximum object size.")

        if math.ceil(file_size / part_size) >= cls.MAX_PARTS:
            part_size = math.ceil(file_size / cls.MAX_PARTS)

        # assert cls.MIN_PART_SIZE <= part_size <= cls.MAX_PART_SIZE

        part_qty, final_part_size = divmod(file_size, part_size)
        if final_part_size == 0:
            final_part_size = part_size
        else:
            part_qty += 1
        if part_qty == 1:
            part_size = final_part_size

        return cls(part_qty, part_size, final_part_size)


client = boto3.client("s3")


SOURCE_BUCKET = "dandi-api-benchmark-dandisets"
SOURCE_OBJECT_KEY = "blobs/f7d/138/f7d1383b-1c28-497e-b7e0-69fa4d55989e"
COPY_SOURCE = f"{SOURCE_BUCKET}/{SOURCE_OBJECT_KEY}"


def gen_object_parts(object_size: int):
    return DandiPartGenerator.for_file_size(object_size)


def copy_part(part: Part):
    ident = str(uuid.uuid4())
    key = f"blobs/{ident[:3]}/{ident[3:6]}/{ident}"

    # Logging
    if part.number % 10 == 0:
        print(f"---- Part {part.number} ---")

    start = time.time()

    # Create upload
    upload_id = client.create_multipart_upload(
        Bucket=SOURCE_BUCKET,
        Key=key,
        ACL="bucket-owner-full-control",
    )["UploadId"]
    finish_upload = time.time()

    # Upload part copy
    part_copy_res = client.upload_part_copy(
        Bucket=SOURCE_BUCKET,
        Key=key,
        UploadId=upload_id,
        CopySource=COPY_SOURCE,
        CopySourceRange=f"bytes={part.offset}-{part.offset + part.size - 1}",
        PartNumber=1,
    )
    finish_copy = time.time()

    # Complete upload
    # complete_upload_start = time.time()
    etag = part_copy_res["CopyPartResult"]["ETag"].strip('"')
    client.complete_multipart_upload(
        Bucket=SOURCE_BUCKET,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [{"ETag": etag, "PartNumber": 1}],
        },
    )
    finish_complete_upload = time.time()

    # Collect
    total = time.time() - start
    time_create_upload = finish_upload - start
    time_part_copy = finish_copy - finish_upload
    time_complete_upload = finish_complete_upload - finish_copy
    return total, time_create_upload, time_part_copy, time_complete_upload


def dissasemble_object(workers, size):
    content_length = size
    # content_length: int = client.head_object(
    #     Bucket=SOURCE_BUCKET, Key=SOURCE_OBJECT_KEY
    # )["ContentLength"]
    parts = list(gen_object_parts(content_length))
    print("WORKERS", workers)
    print(f"TOTAL PARTS: {len(parts)}")
    print("CONTENT LENGTH", content_length)
    print("-------------------------")

    # Timing
    time_part_total = 0
    time_create_upload = 0
    time_part_copy = 0
    time_complete_upload = 0

    start = time.time()

    futures: List[Dict] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for part in parts:
            futures.append(executor.submit(copy_part, part=part))

    finished_parts = [part.result() for part in futures]
    for part in finished_parts:
        _total, _time_create_upload, _time_part_copy, _time_complete_upload = part
        time_part_total += _total
        time_create_upload += _time_create_upload
        time_part_copy += _time_part_copy
        time_complete_upload += _time_complete_upload

    total = time.time() - start
    return (
        parts,
        total,
        time_part_total,
        time_create_upload,
        time_part_copy,
        time_complete_upload,
    )


if __name__ == "__main__":
    filename = f"benchmark_{datetime.now().isoformat()}.csv"
    output_csv = pathlib.Path(__file__).parent / filename
    f = open(output_csv, "w")
    writer = csv.writer(f)
    writer.writerow(OUTPUT_CSV_HEADERS)

    for i in range(1, MAX_WORKERS + 1):
        # total_size = i * kb(150)
        total_size = mb(200)
        (
            parts,
            total,
            part_total,
            create_upload,
            part_copy,
            complete_upload,
        ) = dissasemble_object(i, total_size)

        print(f"Total time taken: {total:.4f} s")

        avg_part_total = part_total / len(parts)
        print(f"Average total part time: {avg_part_total:.4f} s")

        avg_create_upload = create_upload / len(parts)
        print(f"Average time spent creating uploads: {avg_create_upload:.4f} s")

        avg_part_copy = part_copy / len(parts)
        print(f"Average time spent copying parts: {avg_part_copy:.4f} s")

        avg_complete_upload = complete_upload / len(parts)
        print(f"Average time spent completing uploads: {avg_complete_upload:.4f} s")

        # Row data
        writer.writerow(
            [
                i,
                total,
                total_size,
                avg_part_total,
                avg_create_upload,
                avg_part_copy,
                avg_complete_upload,
            ]
        )
        f.flush()

    # Close file
    f.close()
