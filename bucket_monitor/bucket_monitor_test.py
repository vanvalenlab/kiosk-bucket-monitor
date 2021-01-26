# Copyright 2016-2020 The Van Valen Lab at the California Institute of
# Technology (Caltech), with support from the Paul Allen Family Foundation,
# Google, & National Institutes of Health (NIH) under Grant U24CA224309-01.
# All rights reserved.
#
# Licensed under a modified Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.github.com/vanvalenlab/kiosk-bucket-monitor/LICENSE
#
# The Work provided may be used for non-commercial academic purposes only.
# For any other use of the Work, including commercial use, please contact:
# vanvalenlab@gmail.com
#
# Neither the name of Caltech nor the names of its contributors may be used
# to endorse or promote products derived from this software without specific
# prior written permission.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============================================================================
"""Tests for BucketMonitor Class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime

import fakeredis
import pytz
import pytest

import bucket_monitor


class Bunch(object):
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


@pytest.fixture
def redis_client():
    yield fakeredis.FakeStrictRedis(decode_responses='utf8')


class DummyBucket(object):
    def __init__(self, *_, **__):
        pass

    def get_bucket(self, *_):
        return DummyBucket()

    def list_blobs(self, prefix):
        return [
            Bunch(name=prefix,
                  updated=datetime.datetime.now(pytz.UTC),
                  delete=lambda: True),
            Bunch(name='%sfile.tiff' % prefix,
                  updated=datetime.datetime.now(pytz.UTC),
                  delete=lambda: True),
            Bunch(name='%sfile.zip' % prefix,
                  updated=datetime.datetime.now(pytz.UTC),
                  delete=lambda: True)
        ]


class TestBaseBucketMonitor(object):

    def test_init_bad_bucket_protocol(self):
        bad_buckets = ['noprotocol', 'bad:/typo', 'bad//typo']
        for bad_bucket in bad_buckets:
            with pytest.raises(ValueError):
                bucket_monitor.BaseBucketMonitor(bucket=bad_bucket)

    def test_get_storage_api(self):
        # test AWS not implemented yet
        monitor = bucket_monitor.BaseBucketMonitor(bucket='s3://bucket')
        with pytest.raises(NotImplementedError):
            monitor.get_storage_api()

        # test invalid bucket protocol
        monitor = bucket_monitor.BaseBucketMonitor(bucket='bad://bucket')
        with pytest.raises(ValueError):
            monitor.get_storage_api()

    def test_get_all_files(self, mocker):
        # test GKE with stubbed client function
        mocker.patch('google.cloud.storage.Client', DummyBucket)
        monitor = bucket_monitor.BaseBucketMonitor(bucket='gs://bucket')
        prefix = 'test/'
        uploads = monitor.get_all_files(prefix)

        get_names = lambda x: [u.name for u in x]  # pylint: disable=E1101
        names = get_names(uploads)
        assert names == get_names(DummyBucket().list_blobs(prefix))

        # test invalid values for cloud_provider
        monitor = bucket_monitor.BaseBucketMonitor(bucket='bad://bucket')
        uploads = monitor.get_all_files('prefix/')
        assert uploads == []


class TestBucketMonitor(object):
    # pylint: disable=W0621

    def test_scan_bucket_for_new_uploads(self, mocker, redis_client):
        mocker.patch('google.cloud.storage.Client', DummyBucket)
        monitor = bucket_monitor.BucketMonitor(
            redis_client=redis_client,
            bucket='gs://bucket',
            queue='q')
        monitor.scan_bucket_for_new_uploads(prefix='uploads/')

    def test_write_new_redis_key(self, redis_client):
        redis_keys = 'uploads/directupload_previously_uploaded.tifothertext'
        monitor = bucket_monitor.BucketMonitor(
            redis_client=redis_client,
            bucket='gs://bucket',
            queue='q')

        # test invalid web upload
        invalid_file = Bunch(path='uploads/web.tiff',
                             name='uploads/web.tiff',
                             public_url='dummy_url')
        result = monitor.write_new_redis_key(invalid_file, redis_keys)
        assert result == 0

        # test file that has a redis_key
        upload = Bunch(path='uploads/directupload_previously_uploaded.tif',
                       name='uploads/directupload_previously_uploaded.tif',
                       public_url='dummy_url')
        result = monitor.write_new_redis_key(upload, redis_keys)
        assert result == 0

        # test valid direct_upload file_name
        model_name = 'model'
        model_version = 1
        postprocess = 'argmax'
        cuts = 0

        fname = 'uploads/directupload_{}_{}_{}_{}_filename.tiff'.format(
            model_name, model_version, postprocess, cuts)

        upload = Bunch(path=fname, name=fname, public_url=fname)
        result = monitor.write_new_redis_key(upload, redis_keys)
        assert result == 1

        fname = 'uploads/directupload_{}_{}_{}_{}_{}_filename.tiff'.format(
            model_name, model_version, postprocess, cuts,
            'benchmarking4special')
        upload = Bunch(path=fname, name=fname, public_url=fname)
        result = monitor.write_new_redis_key(upload, redis_keys)
        assert result == 4

    def test_create_redis_entry(self, mocker, redis_client):
        mocker.patch('google.cloud.storage.Client', DummyBucket)
        queue = 'q'
        monitor = bucket_monitor.BucketMonitor(
            redis_client=redis_client,
            bucket='gs://bucket',
            queue=queue)

        # test valid direct_upload file_name
        model_name = 'model'
        model_version = 1
        postprocess = 'argmax'
        cuts = 0

        fname = 'directupload_{}_{}_{}_{}_filename.tiff'.format(
            model_name, model_version, postprocess, cuts)

        upload = Bunch(public_url='dummy_url')
        result = monitor.create_redis_entry(upload, fname, fname)
        assert result is True
        hvals = redis_client.hgetall(redis_client.lpop(queue))
        assert hvals['model_name'] == model_name
        assert hvals['model_version'] == str(model_version)
        assert hvals['postprocess_function'] == postprocess
        assert hvals['url'] == 'dummy_url'

        # test bad file_name
        monitor = bucket_monitor.BucketMonitor(
            redis_client=redis_client,
            bucket='gs://bucket',
            queue='q')
        bad_fname = 'regular_filename.tiff'
        result = monitor.create_redis_entry(upload, bad_fname, bad_fname)
        assert result is False
        assert redis_client.lpop(queue) is None


class TestStaleFileBucketMonitor(object):

    def test_scan_bucket_for_stale_files(self, mocker):
        mocker.patch('google.cloud.storage.Client', DummyBucket)
        monitor = bucket_monitor.StaleFileBucketMonitor(bucket='gs://bucket')
        monitor.scan_bucket_for_stale_files(prefix='uploads/', threshold=-1)
