# upload-benchmarks
Benchmarking scripts for our various upload flows

Benchmarks should generally be run on the [DANDI Hub](https://hub.dandiarchive.org/) to ensure a consistent environment and network connection to S3.

### large_file_upload_benchmark
Uploading 10GB file using the normal blob upload API (multipart presigned URLs):
- Total time taken: 60.6492 s
- Time spent transferring bytes to S3: 59.9925 s
- Overall MBit/s: 1319.0617
- Transfer MBit/s: 1333.5003
