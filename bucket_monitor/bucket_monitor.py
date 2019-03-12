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
import time
import uuid
import logging
import datetime

import pytz
from google.cloud import storage
from redis import StrictRedis
from redis.exceptions import ConnectionError


class BucketMonitor(object):
    """Watch a cloud bucket and write an entry to Redis for each new upload.
    """
    def __init__(self,
                 cloud_provider,
                 bucket_name,
                 redis_host='redis-master-0',
                 redis_port=6379,
                 hostname=None):
        self.logger = logging.getLogger(str(self.__class__.__name__))

        # establish cloud connection
        if cloud_provider == 'gke':
            self.bucket_client = storage.Client()
            self.bucket = self.bucket_client.get_bucket(bucket_name)
        elif cloud_provider == 'aws':
            raise NotImplementedError('AWS will be supported soon.')
        else:
            raise ValueError('Invalid cloud_provider: `%s`' % cloud_provider)

        # establish Redis connection
        self.r = StrictRedis(
            host=redis_host,
            port=redis_port,
            decode_responses=True,
            charset='utf-8')

        self.hostname = os.getenv('HOSTNAME') if hostname is None else hostname

        # get initial timestamp to act as a baseline, assume UTC for everything
        self.initial_timestamp = datetime.datetime.now(tz=pytz.UTC)

    def monitor_bucket(self, interval):
        """Checks for new bucket uploads to write to Redis every N seconds.
        """
        while True:
            self.scan_bucket_for_new_uploads()
            time.sleep(interval)

    def scan_bucket_for_new_uploads(self):
        self.logger.info('New loop at %s', self.initial_timestamp)

        # get a timestamp to mark the baseline for the next loop iteration
        soon_to_be_baseline_timestamp = datetime.datetime.now(tz=pytz.UTC)

        # get references to every file starting with "uploads/"
        all_uploads_iterator = self.bucket.list_blobs(prefix='uploads/')
        all_uploads = list(all_uploads_iterator)

        # the oldest one is going to be the "uploads/" folder, so remove that
        upload_times = []
        for upload in all_uploads:
            upload_times.append(upload.updated)
        try:
            earliest_upload = min(upload_times)
        except ValueError:
            self.logger.error('There may not be any folder with the chosen '
                              'prefix. (By default, \"uploads\".)')

        uploads_length = len(all_uploads)
        upload_times_length = len(upload_times)
        for upload in all_uploads:
            if upload.updated == earliest_upload:
                all_uploads.remove(upload)
                upload_times.remove(earliest_upload)

        assert len(all_uploads) == uploads_length - 1
        assert len(upload_times) == upload_times_length - 1

        # keep only the files whose timestamps lie between now and the last
        # iteration of this loop
        new_uploads = []
        for upload in all_uploads:
            if upload.updated > self.initial_timestamp:
                new_uploads.append(upload)
                self.logger.info('Found new upload: %s', upload)

        # for each of these new uploads, parse necessary information from the
        # filename and write an appropriate entry to Redis
        # Before writing, also check that no hash already in Redis contains
        # this file's name. (We seem to have a problem with double
        # entries in Redis.)
        all_keys = self._get_all_redis_keys()
        self._write_new_redis_keys(new_uploads, all_keys)

        # update baseline timestamp
        self.initial_timestamp = soon_to_be_baseline_timestamp

    def _get_all_redis_keys(self, interval=5):
        """Queries Redis for all keys and return them joined with the delimiter

        Args:
            interval: time to wait before retrying redis query.

        Returns:
            a set of all keys in Redis.
        """
        retrying = True
        while retrying:
            try:
                all_keys = self.r.keys()
                retrying = False
            except ConnectionError as err:
                # Issue connecting to Redis. Retry until connection established.
                self.logger.warn('Trouble connecting to Redis: %s - %s.\n'
                                 'Retrying in %s seconds...',
                                 type(err).__name__, err, interval)
                time.sleep(interval)

        return set(list(all_keys))  # set of all keys in Redis.

    def _write_new_redis_keys(self, new_uploads, all_keys):
        for upload in new_uploads:
            # verify that we're dealing with a direct upload, and not a web
            # upload
            re_filename = '(uploads(?:/|%2F))(directupload_.+)$'
            try:
                upload_filename = re.search(re_filename, upload.path).group(2)
            except AttributeError as err:
                # this isn't a directly uploaded file
                # or its filename was formatted incorrectly
                self.logger.debug('Failed on filename of %s. Error %s: %s',
                                  upload.path, type(err).__name__, err)
                continue

            # check for presence of filename in Redis already
            if upload_filename in all_keys:
                self.logger.warn('%s tried to get uploaded a second time.',
                                 upload_filename)
                continue

            # Is this a special "benchmarking" direct upload?
            benchmarking_result = re.search('benchmarking([0-9]+)special',
                                            upload_filename)

            if benchmarking_result is None:
                # standard direct upload
                self._create_single_redis_entry(
                    upload, upload_filename, upload_filename)
            else:
                # "benchmarking" direct upload
                self._create_multiple_redis_entries(
                    upload, upload_filename,
                    number_of_images=benchmarking_result.group(1))

    def _create_multiple_redis_entries(self,
                                       upload,
                                       upload_filename,
                                       number_of_images):
        # make a number of redis entries corresponding to the number found in
        # the benchmarking section of the filename
        root, ext = os.path.splitext(upload_filename)
        for img_num in range(int(number_of_images)):
            current_upload_filename = '{basename}{num}{ext}'.format(
                basename=root, num=img_num, ext=ext)

            self._create_single_redis_entry(
                upload, current_upload_filename, upload_filename)

    def _create_single_redis_entry(self,
                                   upload,
                                   modified_upload_filename,
                                   unmodified_upload_filename):
        # dictionary for uploading to Redis
        field_dict = {}
        field_dict['url'] = upload.public_url
        field_dict['input_file_name'] = 'uploads/' + unmodified_upload_filename
        field_dict['status'] = 'new'
        # filename schema: modelname_modelversion_ppfunc_cuts_etc
        re_fields = 'directupload_([^_]+)_([0-9]+)_([^_]+)_([0-9]+)_.+$'
        fields = re.search(re_fields, unmodified_upload_filename)
        try:
            field_dict['model_name'] = fields.group(1)
            field_dict['model_version'] = fields.group(2)
            field_dict['postprocessing_function'] = fields.group(3)
            field_dict['cuts'] = fields.group(4)
        except AttributeError:
            self.logger.warn('Failed on fields of %s.', modified_upload_filename)
            return 0

        field_dict['identity_upload'] = self.hostname
        field_dict['timestamp_upload'] = time.time() * 1000
        redis_key = 'predict_{hash}_{filename}'.format(
            hash=uuid.uuid4().hex,
            filename=modified_upload_filename)

        self._write_redis_entry(redis_key, field_dict, modified_upload_filename)

    def _write_redis_entry(self,
                           redis_key,
                           field_dict,
                           upload_filename,
                           interval=5):
        retrying = True
        while retrying:
            try:
                self.r.hmset(redis_key, field_dict)
                retrying = False
            except ConnectionError as err:
                # For some reason, we're unable to connect to Redis
                # right now. Keep trying until we can.
                self.logger.warn('Encountered %s while connecting to Redis: %s.'
                                 '  Retrying in %s seconds...',
                                 type(err).__name__, err, interval)
                time.sleep(interval)

        self.logger.debug('Wrote Redis entry for %s.', upload_filename)
