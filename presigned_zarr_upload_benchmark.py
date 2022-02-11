"""
Benchmark presigned zarr uploads using dummy data.

The time taken to complete the upload is included, so if the API server is calculating checksums that will factor into the performance.
At the time this was written a temporary API server was stood up that did not calculate checksums specifically to determine how efficient uploads were without them.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from hashlib import md5
import os
from pathlib import Path
import shutil
import threading
import time

import click
from dandischema.digests.dandietag import mb, gb, tb
import requests

API_ROOT = 'https://api-benchmark.dandiarchive.org'


def kb(bytes_size: int) -> int:
    return bytes_size * 2**10


class Timer:
    def __init__(self):
        self._start = time.time()
        self._last_mark = self._start

    def mark(self):
        now = time.time()
        lap = now - self._last_mark
        self._last_mark = now
        return lap

    @property
    def total(self):
        return time.time() - self._start


def generate_source_files(root_path: Path, file_count, file_size):
    print(f'Generating {file_count} files of size {file_size/1_000_000}MB in {root_path}')
    for i in range(0, file_count):
        data = os.urandom(file_size)
        hasher = md5()
        hasher.update(data)
        checksum = hasher.hexdigest()
        file_id = f'{i:010}'
        # This monstrosity turns 0123456789 into 01/23/45/67/89
        file_dir = '/'.join([f'{a}{b}' for a, b in zip(file_id[::2], file_id[1::2])])
        os.makedirs(root_path / file_dir, exist_ok=True)
        with open(root_path / file_dir / checksum, 'wb') as f:
            f.write(data)


def scan_source_files(root_path: Path):
    print(f'Scanning the contents of {root_path} for upload')
    files = []
    for dirpath, _, filenames in os.walk(root_path):
        if filenames:
            dirpath = dirpath[len(str(root_path)) + 1 :]
            files += [
                {
                    'path': f'{dirpath}/{filename}',
                    'etag': filename,
                }
                for filename in filenames
            ]

    return files


def initialize_upload(session, zarr_id, files):
    print(f'Initializing upload of {len(files)} files to {zarr_id}')
    resp = session.post(f'{API_ROOT}/api/zarr/{zarr_id}/upload/', json=files)
    assert resp.status_code == 200
    return resp.json()


def upload_files(source_dir, uploads, workers):
    print(f'Uploading {len(uploads)} files with {workers} workers')
    locals = threading.local()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                upload_file, locals, Path(source_dir) / upload['path'], upload['upload_url']
            )
            for upload in uploads
        ]
        total_size = sum(fut.result() for fut in as_completed(futures))
        print(f'Finished uploading {total_size/1_000_000}MB')
        return total_size


def upload_file(locals: threading.local, file_path: Path, upload_url: str):
    if getattr(locals, 'session', None) is None:
        locals.session = requests.session()
    session = locals.session
    with open(file_path, 'rb') as f:
        resp = session.put(
            upload_url,
            data=f,
            headers={'X-Amz-ACL': 'bucket-owner-full-control'},
        )
        assert resp.status_code == 200
        return int(resp.request.headers['Content-Length'])


def complete_upload(session, zarr_id):
    print('Completing the upload')
    resp = session.post(f'{API_ROOT}/api/zarr/{zarr_id}/upload/complete/')
    assert resp.status_code == 201


@click.command()
@click.option(
    "--api-key",
    "-a",
    "api_key",
    required=True,
    help="The DANDI API key to use for the upload.",
)
@click.option(
    "--output",
    "-o",
    "filename",
    default=f"benchmark_{datetime.now().isoformat()}.csv",
    show_default=True,
    help="Output file name.",
)
@click.option(
    "--zarr-id",
    "zarr_id",
    default=None,
    show_default=True,
    help="The zarr file to upload to.",
)
@click.option(
    "--workers",
    "workers",
    default=50,
    show_default=True,
    help="The number of workers.",
)
@click.option(
    "--part-size",
    "part_size",
    default=kb(150),
    show_default="150 KiB",
    help="The size of each part.",
)
@click.option(
    "--total-size",
    "total_size",
    default=mb(200),
    show_default="200 MiB",
    help="The size of the total object (must be less than total actual object size).",
)
@click.option(
    "--generate",
    "generate",
    default=False,
    show_default=True,
    is_flag=True,
    help="Generate the data to be uploaded.",
)
@click.option(
    "--complete",
    "complete",
    default=False,
    show_default=True,
    is_flag=True,
    help="Complete the upload, which involves updating checksums.",
)
@click.argument(
    "source_dir",
    default=Path('upload'),
    type=click.Path(),
)
def upload_test(
    api_key,
    filename,
    zarr_id,
    workers,
    part_size,
    total_size,
    generate,
    complete,
    source_dir: Path,
):
    source_dir = Path(source_dir)
    if generate:
        if source_dir.exists():
            click.confirm(f'Are you sure you want to delete {source_dir}?', abort=True)
            shutil.rmtree(source_dir)
        generate_source_files(source_dir, total_size // part_size, part_size)
    session = requests.session()
    session.headers['Authorization'] = f'token {api_key}'
    if zarr_id is None:
        print('Creating a new zarr archive')
        resp = session.post(f'{API_ROOT}/api/zarr/', json={'name': 'test_upload'})
        assert resp.status_code == 200
        zarr_id = resp.json()['zarr_id']
        print(f'Created new zarr archive {zarr_id}')

    files = scan_source_files(source_dir)

    # Try deleting the upload preemptively just in case one already existed.
    session.delete(f'{API_ROOT}/api/zarr/{zarr_id}/upload/')

    timer = Timer()
    uploads = initialize_upload(session, zarr_id, files)
    time_initialize = timer.mark()
    total_size = upload_files(source_dir, uploads, workers)
    time_upload = timer.mark()
    if complete:
        complete_upload(session, zarr_id)
        time_complete = timer.mark()
    time_total = timer.total

    print(f'Initialize took {time_initialize:.4f} seconds')
    print(
        f'Upload took {time_upload:.4f} seconds ({(total_size / time_upload)/1_000_000:.4f} MB/s, {(total_size / time_upload)*8/1_000_000:.4f} Mbit/s)'
    )
    if complete:
        print(f'Completion took {time_complete:.4f} seconds')
    print(
        f'Total time taken: {time_total:.4f} seconds ({(total_size / time_total)/1_000_000:.4f} MB/s, {(total_size / time_total)*8/1_000_000:.4f} Mbit/s)'
    )


if __name__ == '__main__':
    upload_test()
