"""
Benchmark uploading a single large file to S3 using the DANDI API.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import threading
import time

import requests

API_KEY = os.getenv('API_KEY')
API_KEY = '99e91a965c0d1dc8e76a7094e34e5c402c6c497f' # TODO remove me
API_ROOT = 'https://api-benchmark.dandiarchive.org'
ETAG = 'f' * 32 # Dummy value, since we don't want to have to checksum the random data in advance
DANDISET = '000001'
MAX_WORKERS = 10

def new_s3_session():
    session = requests.session()
    return session

def new_django_session():
    session = requests.session()
    session.headers['Authorization'] = f'token {API_KEY}'
    return session

local = threading.local()

def upload_part(part):
    if not getattr(local, 'session', None):
        session = new_s3_session()
        local.session = session
    part_number = part['part_number']
    part_size = part['size']
    print(f'uploading part {part_number} of size {part_size / 1_000_000} MB')
    upload_url = part['upload_url']
    data = os.urandom(part_size)
    resp = local.session.put(upload_url, data=data, headers={
        'content-length': str(part_size),
        'host': 'dandi-api-benchmark-dandisets.s3.amazonaws.com',
    })
    assert resp.status_code == 200
    etag = resp.headers['etag']
    return {'part_number': part_number, 'size': part_size, 'etag': etag}


def upload_file(size: int):
    start = time.time()
    django_session = new_django_session()
    resp = django_session.post(f'{API_ROOT}/api/uploads/initialize/', json={
        'contentSize': size,
        'digest': {
            'algorithm': 'dandi:dandi-etag',
            'value': ETAG,
        },
        'dandiset': DANDISET
    })
    assert resp.status_code == 200
    payload = resp.json()
    upload_id = payload['upload_id']
    parts = payload['parts']
    print(f'Uploading file of size {size/1_000_000} MB in {len(parts)} parts')
    initialize_mark = time.time()

    threads = min(len(parts), MAX_WORKERS)
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [
            # executor.submit(upload_file, upload_url)
            # for upload_url in upload_urls
            executor.submit(upload_part, part)
            for part in parts
        ]
        transferred_parts = []
        for fut in as_completed(futures):
            exception = fut.exception()
            if exception:
                raise exception
            transferred_parts.append(fut.result())
    upload_mark = time.time()

    # Get the presigned complete URL
    completion = django_session.post(
        f'{API_ROOT}/api/uploads/{upload_id}/complete/',
        json={
            'parts': transferred_parts,
        },
    ).json()
    completion_mark = time.time()

    # Complete the upload to the object store
    completion_response = requests.post(completion['complete_url'], data=completion['body'], headers={'host': 'dandi-api-benchmark-dandisets.s3.amazonaws.com'})
    assert completion_response.status_code == 200

    end = time.time()
    return end - start, initialize_mark - start, upload_mark-initialize_mark, completion_mark-upload_mark, end-completion_mark

if __name__ == '__main__':
    size = 10_000_000
    total, initialize, upload, completion_prep, completion = upload_file(size)
    print(f'Total time taken: {total:.4f} s')
    print(f'Time spent transferring bytes to S3: {upload:.4f} s')
    print(f'Overall MBit/s: {(size*8/1_000_000)/ total:.4f}')
    print(f'Transfer MBit/s: {(size*8/1_000_000)/ upload:.4f}')