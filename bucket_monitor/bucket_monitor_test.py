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

import redis
import pytest

from bucket_monitor import BucketMonitor


class DummyRedis(object):

    def __init__(self, fail_tolerance=2):
        self.fail_count = 0
        self.fail_tolerance = fail_tolerance

    def keys(self):
        if self.fail_count < self.fail_tolerance:
            self.fail_count += 1
            raise redis.exceptions.ConnectionError('thrown on purpose')
        return ['abc', '123']

    def hgetall(self, _):
        if self.fail_count < self.fail_tolerance:
            self.fail_count += 1
            raise redis.exceptions.ConnectionError('thrown on purpose')
        return {'key': 'value'}

    def hmset(self, *_):
        if self.fail_count < self.fail_tolerance:
            self.fail_count += 1
            raise redis.exceptions.ConnectionError('thrown on purpose')
        return 1


class TestBucketMonitor(object):

    def test_init(self):
        # test bad cloud_providers
        with pytest.raises(ValueError):
            BucketMonitor('invalid', 'bucket', None)

        # test upload_prefix formatting
        bm1 = BucketMonitor('gke', 'bucket', None, upload_prefix='//uploads')
        bm2 = BucketMonitor('aws', 'bucket', None, upload_prefix='uploads')
        bm3 = BucketMonitor('gke', 'bucket', None, upload_prefix='/uploads/')
        bm4 = BucketMonitor('aws', 'bucket', None, upload_prefix='uploads/')
        assert all(i.upload_prefix == 'uploads/' for i in (bm1, bm2, bm3, bm4))

    def test_parse_predict_fields(self):
        # single image
        single = '{pre}_{name}_{version}_{post}_{cuts}_{img}'.format(
            pre='directupload', name='watershednuclearnofgbg41f16',
            version=0, post='watershed', cuts=0, img='image_0.png')
        # multiple image uploads
        multi = '{pre}_{name}_{version}_{post}_{cuts}_{n}_{img}'.format(
            pre='directupload', name='watershednuclearnofgbg41f16',
            version=0, post='watershed', cuts=0, img='image_0.png',
            n='benchmarking10000special')

        other = 'this is not a formatted string'

        bm = BucketMonitor('aws', 'bucket', DummyRedis(),
                           redis_retry_interval=0.01)

        single_fields = bm.parse_predict_fields(single)

        multi_fields = bm.parse_predict_fields(multi)
        # should have the same redis fields
        assert single_fields == multi_fields

        with pytest.raises(AttributeError):
            bm.parse_predict_fields(other)

    # def test_scan_bucket_for_new_uploads(self):
    #     bm = BucketMonitor('gke', 'bucket', DummyRedis(),
    #                        redis_retry_interval=0.01)
    #
    #     single = '{pre}_{name}_{version}_{post}_{cuts}_{img}'.format(
    #         pre='directupload', name='watershednuclearnofgbg41f16',
    #         version=0, post='watershed', cuts=0, img='image_0.png')
    #
    #     # override `enumerate_uploads` as we don't want to ping a real bucket
    #     bm.enumerate_uploads = lambda: [
    #         'uploads/',
    #         'uploads/{}'.format(single)
    #     ]
    #
    #     bm.scan_bucket_for_new_uploads()

    def test_keys(self):
        redis_client = DummyRedis(fail_tolerance=2)
        bm = BucketMonitor('gke', 'bucket', redis_client,
                           redis_retry_interval=0.01)

        keys = bm.keys()
        assert keys == ['abc', '123']
        assert bm.r.fail_count == redis_client.fail_tolerance

    def test_hgetall(self):
        redis_client = DummyRedis(fail_tolerance=2)
        bm = BucketMonitor('gke', 'bucket', redis_client,
                           redis_retry_interval=0.01)

        data = bm.hgetall('redis_hash')
        assert data == {'key': 'value'}
        assert bm.r.fail_count == redis_client.fail_tolerance

    def test_hmset(self):
        redis_client = DummyRedis(fail_tolerance=3)
        bm = BucketMonitor('gke', 'bucket', redis_client,
                           redis_retry_interval=0.01)
        bm.hmset('rhash', {'key': 'value'})
        assert bm.r.fail_count == redis_client.fail_tolerance
