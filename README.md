# upload-benchmarks
Benchmarking scripts for our various upload flows

Benchmarks should generally be run on the [DANDI Hub](https://hub.dandiarchive.org/) with a "Large" container to ensure a consistent environment and network connection to S3.

Tests were run against a separate Heroku instance set up specifically for benchmarking. This makes clean up quick and convenient. See https://github.com/dandi/dandi-infrastructure/pull/115 for details.

### presigned_zarr_upload_benchmark
This benchmarks uploading zarr-like data using the zarr upload API. 150KB file chunks are used.

The server used to run this benchmark had checksumming disabled so that the upload was limited to moving bytes into S3.

Uploading a batch of 10,000 150KB files (1.5 GB total) with 50 threads:
- 55 seconds
- 216 MBit/s

### large_file_upload_benchmark
This benchmarks uploading a single large file using the standard blob upload API. Randomly generated data is used.

Uploading 10GB file with 50 threads:
- Total time taken: 60.6492 s
- Time spent transferring bytes to S3: 59.9925 s
- Overall MBit/s: 1319.0617
- Transfer MBit/s: 1333.5003


### zarr_file_digestion_benchmark
This benchmarks splitting up a file that already exists in S3 into smaller parts. 

Note that this is happening with multiple threads on a single container. In the celery worker implementation, each worker is it's own container, which would help with client side network saturation.

Splitting up a 10GB file into 65,105 150KB parts using 50 threads:
- Total time taken: 809.0153 s (13.5 min)
- Average time total per part: 0.6150 s
  - Average time spent creating uploads: 0.1655 s
  - Average time spent copying parts: 0.2232 s
  - Average time spent completing uploads: 0.2263 s
