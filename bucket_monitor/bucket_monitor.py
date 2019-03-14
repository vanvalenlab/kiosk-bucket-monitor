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
"""BucketMonitor Class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import json
import time
import uuid
import logging
import datetime

import pytz
import redis
from google.cloud import storage


class BucketMonitor(object):
    """Watch a cloud bucket and write an entry to Redis for each new upload.
    """
    def __init__(self,
                 cloud_provider,
                 bucket_name,
                 redis_client,
                 job_type='predict',
                 hostname=None,
                 redis_retry_interval=5,
                 upload_prefix='uploads/'):
        if str(cloud_provider).lower() not in {'gke', 'aws'}:
            raise ValueError('Invalid cloud_provider: `%s`' % cloud_provider)

        self.cloud_provider = cloud_provider
        self.r = redis_client  # establish Redis connection
        self.bucket_name = bucket_name
        self.redis_retry_interval = redis_retry_interval
        self.job_type = job_type
        self.hostname = hostname

        # get initial timestamp to act as a baseline, assume UTC for everything
        self.initial_timestamp = datetime.datetime.now(tz=pytz.UTC)

        self.upload_prefix = upload_prefix
        if not self.upload_prefix.endswith('/'):
            self.upload_prefix += '/'  # append trailing "/"
        while self.upload_prefix.startswith('/'):
            self.upload_prefix = self.upload_prefix[1:]  # remove leading "/"

        self.logger = logging.getLogger(str(self.__class__.__name__))

    def enumerate_uploads(self):
        """Get a list of all uploads inside the cloud bucket's `upload_prefix`.

        Returns:
            list of objects inside the bucket/upload_prefix.
        """
        if self.cloud_provider == 'gke':
            bucket_client = storage.Client()
            bucket = bucket_client.get_bucket(self.bucket_name)
            return list(bucket.list_blobs(prefix=self.upload_prefix))
        elif self.cloud_provider == 'aws':
            raise NotImplementedError('AWS is not currently supported')
        else:
            raise ValueError('Invalid cloud_provider: %s' % self.cloud_provider)

    def scan_bucket_for_new_uploads(self):
        self.logger.debug('New loop at %s', self.initial_timestamp)

        # get a timestamp to mark the baseline for the next loop iteration
        soon_to_be_baseline_timestamp = datetime.datetime.now(tz=pytz.UTC)

        # get references to every file starting with `upload_prefix`
        all_uploads = self.enumerate_uploads()

        # the oldest one is going to be the `upload_prefix` folder, remove it
        upload_times = []
        for upload in all_uploads:
            upload_times.append(upload.updated)
        try:
            earliest_upload = min(upload_times)
        except ValueError:
            self.logger.error('No uploads found.  The value for upload_prefix:'
                              ' ``%s` may be incorrect.', self.upload_prefix)

        uploads_length = len(all_uploads)
        upload_times_length = len(upload_times)
        for upload in all_uploads:
            if upload.updated == earliest_upload:
                all_uploads.remove(upload)
                upload_times.remove(earliest_upload)
        # make sure we only removed one entry
        assert len(all_uploads) == uploads_length - 1
        assert len(upload_times) == upload_times_length - 1

        successes = 0
        all_keys = set(self.keys())  # get all the keys in Redis
        for upload in all_uploads:
            # only write new keys for uploads between now and the last loop
            if upload.updated > self.initial_timestamp:
                self.logger.info('Found new upload: %s', upload)
                # parse info from the filename, and write data to Redis.
                success = self._write_new_redis_keys(upload, all_keys)
                successes += int(success)

        # update baseline timestamp
        self.initial_timestamp = soon_to_be_baseline_timestamp
        if successes:
            self.logger.info('Successfully added %s keys to Redis', successes)

    def keys(self):
        """Wrapper function for redis.keys().

        Retries on ConnectionError every N seconds.

        Args:
            interval: time to wait before retrying redis query.

        Returns:
            a set of all keys in Redis.
        """
        while True:
            try:
                all_keys = self.r.keys()
                break
            except redis.exceptions.ConnectionError as err:
                # Retry until Redis connection is established.
                self.logger.warn('Connection to Redis could not be established'
                                 ' due to %s: %s. Retrying in %s seconds...',
                                 type(err).__name__, err,
                                 self.redis_retry_interval)

                time.sleep(self.redis_retry_interval)

        return all_keys

    def _write_new_redis_keys(self, upload, redis_keys):
        """Parse the upload information and write required data to Redis.

        There seems to be an issue with double Redis entries, so before writing,
        check that no hash already in Redis contains the filename.

        Args:
            upload: Object in cloud bucket.
            redis_keys: a collection of hashes currently in Redis.

        Returns: Count of keys written to Redis.
        """
        # verify that we're dealing with a direct upload, and not a web upload
        re_filename = '({}(?:/|%2F))(directupload_.+)$'.format(
            self.upload_prefix[:-1])

        try:
            upload_filename = re.search(re_filename, upload.path).group(2)
        except AttributeError as err:
            # this isn't a directly uploaded file
            # or its filename was formatted incorrectly
            self.logger.debug('Failed on filename of %s. Error %s: %s',
                              upload.path, type(err).__name__, err)
            return 0

        # check for presence of filename in Redis already
        if upload_filename in redis_keys:
            self.logger.warn('%s tried to get uploaded a second time.',
                             upload_filename)
            return 0

        # Is this a special "benchmarking" direct upload?
        benchmarking_result = re.search('benchmarking([0-9]+)special',
                                        upload_filename)

        if benchmarking_result is None:  # "standard" direct upload
            return self._create_redis_entry(
                upload, upload_filename, upload_filename)

        # "benchmarking" direct upload
        count = 0
        root, ext = os.path.splitext(upload_filename)
        for img_num in range(int(benchmarking_result.group(1))):
            current_upload_filename = '{basename}{num}{ext}'.format(
                basename=root, num=img_num, ext=ext)

            count += self._create_redis_entry(upload, current_upload_filename,
                                              upload_filename)
        return count

    def parse_predict_fields(self, filename):
        field_dict = {}
        # filename schema: modelname_modelversion_ppfunc_cuts_etc
        re_fields = 'directupload_([^_]+)_([0-9]+)_([^_]+)_([0-9]+)_.+$'
        fields = re.search(re_fields, filename)
        field_dict['model_name'] = fields.group(1)
        field_dict['model_version'] = fields.group(2)
        field_dict['postprocessing_function'] = fields.group(3)
        field_dict['cuts'] = fields.group(4)
        return field_dict

    def _create_redis_entry(self, upload, new_filename, original_filename):
        # dictionary for uploading to Redis
        if self.job_type == 'predict':
            try:
                field_dict = self.parse_predict_fields(original_filename)
            except AttributeError:
                # this isn't a directly uploaded file
                # or its filename was formatted incorrectly
                self.logger.warn('Failed on filename of ``%s`.', upload.path)
                return 0

        elif self.job_type == 'notebook':
            field_dict = {}

        else:
            raise NotImplementedError('Job type of %s is not supported yet' %
                                      self.job_type)

        # standard fields for all job types
        field_dict['url'] = upload.public_url
        field_dict['input_file_name'] = os.path.join(
            self.upload_prefix, original_filename)
        field_dict['identity_upload'] = self.hostname
        field_dict['timestamp_upload'] = time.time() * 1000
        field_dict['status'] = 'new'
        field_dict['type'] = self.job_type

        redis_key = '{job_type}_{hash}_{filename}'.format(
            job_type=self.job_type,  # TODO: build other job types?
            hash=uuid.uuid4().hex,
            filename=new_filename)

        self.hmset(redis_key, field_dict)
        self.logger.info('Wrote Redis entry for %s: %s', redis_key,
                         json.dumps(self.hgetall(redis_key), indent=4))
        return 1

    def hgetall(self, key):
        """Wrapper function for redis.hgetall(key).

        Retries on ConnectionError every N seconds.

        Args:
            interval: time to wait before retrying redis query.

        Returns:
            all redis values for the given key
        """
        while True:
            try:
                key_values = self.r.hgetall(key)
                break
            except redis.exceptions.ConnectionError as err:
                self.logger.warn('Connection to Redis could not be established'
                                 ' due to %s: %s. Retrying in %s seconds...',
                                 type(err).__name__, err,
                                 self.redis_retry_interval)

                time.sleep(self.redis_retry_interval)
        return key_values

    def hmset(self, redis_key, fields):
        """Wrapper function for redis.hmset(key, data).

        Retries on ConnectionError every N seconds.

        Args:
            interval: time to wait before retrying redis query.

        Returns:
            a set of all keys in Redis.
        """
        while True:
            try:
                self.r.hmset(redis_key, fields)
                break
            except redis.exceptions.ConnectionError as err:
                # Can't connect to Redis, retry until connection established.
                self.logger.warn('Connection to Redis could not be established'
                                 ' due to %s: %s. Retrying in %s seconds...',
                                 type(err).__name__, err,
                                 self.redis_retry_interval)

                time.sleep(self.redis_retry_interval)
