# Copyright 2016-2019 The Van Valen Lab at the California Institute of
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

import pytz
import pytest

import bucket_monitor


class Bunch(object):
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class DummyRedis(object):
    def __init__(self):
        self.hvals = {}

    def lpush(self, *_, **__):
        return True

    def keys(self):
        return [
        ]

    def hmset(self, _, hvals):
        self.hvals = hvals
        return hvals

    def hgetall(self, _):
        return {
        }


class DummyBucket(object):
    def __init__(self, *_, **__):
        pass

    def get_bucket(self, *_):
        return DummyBucket()

    def list_blobs(self, prefix):
        return [
            Bunch(name=prefix,
                  updated=datetime.datetime.now(pytz.UTC)),
            Bunch(name='%sfile.tiff' % prefix,
                  updated=datetime.datetime.now(pytz.UTC)),
            Bunch(name='%sfile.zip' % prefix,
                  updated=datetime.datetime.now(pytz.UTC))
        ]


class TestBucketMonitor(object):

    def test_get_storage_api(self):
        # test invalid values for cloud_provider
        monitor = bucket_monitor.BucketMonitor('redis', 'bad', 'bucket', 'q')
        with pytest.raises(ValueError):
            monitor.get_storage_api()

    def test_get_all_uploads(self):
        # test GKE with stubbed client function
        monitor = bucket_monitor.BucketMonitor('redis', 'gke', 'bucket', 'q')
        monitor.get_storage_api = DummyBucket
        prefix = 'test/'
        uploads = monitor.get_all_uploads(prefix)

        get_names = lambda x: [u.name for u in x]  # pylint: disable=E1101
        names = get_names(uploads)
        assert names == get_names(DummyBucket().list_blobs(prefix))

        # test invalid values for cloud_provider
        monitor = bucket_monitor.BucketMonitor('redis', 'bad', 'bucket', 'q')
        monitor.get_storage_api = DummyBucket
        uploads = monitor.get_all_uploads('prefix/')
        assert uploads == []

    def test_scan_bucket_for_new_uploads(self):
        redis_client = DummyRedis()
        monitor = bucket_monitor.BucketMonitor(
            redis_client, 'gke', 'bucket', 'q')
        monitor.get_storage_api = DummyBucket
        monitor.scan_bucket_for_new_uploads(prefix='uploads/')

    def test_write_new_redis_key(self):
        redis_keys = 'uploads/directupload_previously_uploaded.tifothertext'
        redis_client = DummyRedis()
        monitor = bucket_monitor.BucketMonitor(
            redis_client, 'gke', 'bucket', 'q')

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

    def test_create_redis_entry(self):
        redis_client = DummyRedis()
        monitor = bucket_monitor.BucketMonitor(
            redis_client, 'gke', 'bucket', 'q')
        monitor.get_storage_api = DummyBucket

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
        assert redis_client.hvals['model_name'] == model_name
        assert redis_client.hvals['model_version'] == str(model_version)
        assert redis_client.hvals['postprocess_function'] == postprocess
        assert redis_client.hvals['cuts'] == str(cuts)
        assert redis_client.hvals['url'] == 'dummy_url'

        # test bad file_name
        redis_client = DummyRedis()
        monitor = bucket_monitor.BucketMonitor(
            redis_client, 'gke', 'bucket', 'q')
        bad_fname = 'regular_filename.tiff'
        result = monitor.create_redis_entry(upload, bad_fname, bad_fname)
        assert result is False
        assert redis_client.hvals == {}
