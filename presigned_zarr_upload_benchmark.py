"""
Benchmark presigned zarr uploads using dummy data.

The time taken to complete the upload is included, so if the API server is calculating checksums that will factor into the performance.
At the time this was written a temporary API server was stood up that did not calculate checksums specifically to determine how efficient uploads were without them.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from hashlib import md5
import time

import requests

API_KEY = 'replace me'
API_ROOT = 'https://api-staging.dandiarchive.org'
ZARR_ID = 'replace me'
MAX_WORKERS = 1
BATCH_SIZE = 1000
CONTENT_LENGTH = 150_000  # 150KB of data
FILE_DATA = b'x' * CONTENT_LENGTH
hasher = md5()
hasher.update(FILE_DATA)
checksum = hasher.hexdigest()

session = requests.session()
session.headers['Authorization'] = f'token {API_KEY}'


def upload_file(upload_url):
    resp = requests.put(
        upload_url,
        data=FILE_DATA,
        headers={'X-Amz-ACL': 'bucket-owner-full-control'},
    )
    assert resp.status_code == 200

def upload_file_from_disk(upload_url, path):
    with open(path, 'rb') as f:
        resp = requests.put(
            upload_url,
            data=f.read(),
            headers={'X-Amz-ACL': 'bucket-owner-full-control'},
        )
        assert resp.status_code == 200

def upload_batch(batch_size):
    # Cancel any existing uploads if there are any
    resp = session.delete(f'{API_ROOT}/api/zarr/{ZARR_ID}/upload/')
    print(resp)

    # Get the presigned upload URLs
    start = time.time()
    req = [
        {
            'path': f'{int(time.time())}/{i+1}',
            'etag': checksum,
        }
        for i in range(0, batch_size)
    ]
    resp = session.post(f'{API_ROOT}/api/zarr/{ZARR_ID}/upload/', json=req)
    presigns = resp.json()
    print(presigns[0])
    def filepath(i):
        j = str(i).zfill(len(str(batch_size)))
        k = "/".join([c for c in j[:-1]])
        return f'dummy_data/{k}/{j}'
    datums = [
        {'upload_url': ps['upload_url'], 'file': filepath(i)}
        for ps, i in zip(presigns, range(0, batch_size))
    ]
    print(datums[0])

    #upload_urls = [obj['upload_url'] for obj in resp.json()]
    print(f'Presigned in {time.time()-start} seconds')

    # Upload the dummy data
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            # executor.submit(upload_file, upload_url)
            # for upload_url in upload_urls
            executor.submit(upload_file_from_disk, d['upload_url'], d['file'])
            for d in datums
        ]
        for fut in as_completed(futures):
            pass
    print(f'Uploaded in {time.time()-start} seconds')
    
    # Complete the upload
    resp = session.post(f'{API_ROOT}/api/zarr/{ZARR_ID}/upload/complete/')
    assert resp.status_code == 201
    end = time.time()
    return end - start, batch_size * CONTENT_LENGTH

if __name__ == '__main__':
    time, size = upload_batch(BATCH_SIZE)
    print(f'Finished in {time} seconds')
    print(f'{size/(time*1_000_000)} MB/s')
    print(f'{size*60/(time*1_000_000)} MB/min')