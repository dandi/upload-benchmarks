# upload-benchmarks
Benchmarking scripts for our various upload flows

Benchmarks should generally be run on the [DANDI Hub](https://hub.dandiarchive.org/) to ensure a consistent environment and network connection to S3.

### large_file_upload_benchmark
Uploading 10GB file using the normal blob upload API (multipart presigned URLs):
- Total time taken: 60.6492 s
- Time spent transferring bytes to S3: 59.9925 s
- Overall MBit/s: 1319.0617
- Transfer MBit/s: 1333.5003


### zarr_file_digestion_benchmark
Splitting up a 10GB file into 65,105 150KB parts
- Total time taken: 809.0153 s (13.5 min)
- Average time total per part: 0.6150 s
  - Average time spent creating uploads: 0.1655 s
  - Average time spent copying parts: 0.2232 s
  - Average time spent completing uploads: 0.2263 s
