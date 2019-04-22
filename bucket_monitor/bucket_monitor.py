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
"""Bucket Monitor Class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import time
import uuid
import datetime
import logging

import pytz
from google.cloud import storage


class BucketMonitor(object):
    """Watches a bucket for new uploads and adds data for each to Redis."""

    def __init__(self, redis_client, cloud_provider, bucket_name):
        # establish cloud connection
        if cloud_provider == 'gke':
            self.bucket_client = storage.Client()
            self.bucket = self.bucket_client.get_bucket(bucket_name)
        elif cloud_provider == 'aws':
            raise NotImplementedError('{} does not yet support {}.'.format(
                self.__class__.__name__, cloud_provider))
        else:
            raise ValueError('Invalid value for `cloud_provider`: {}.'.format(
                cloud_provider))

        self.redis_client = redis_client
        self.logger = logging.getLogger(str(self.__class__.__name__))

        # read in environment variables
        self.INTERVAL = os.environ['INTERVAL']
        self.HOSTNAME = os.environ['HOSTNAME']

        # get initial timestamp to act as a baseline, assuming UTC for everything
        self.initial_timestamp = datetime.datetime.now(tz=pytz.UTC)

    def scan_bucket_for_new_uploads(self):
        self.logger.info('New loop at %s', self.initial_timestamp)

        # get a timestamp to mark the baseline for the next loop iteration
        soon_to_be_baseline_timestamp = datetime.datetime.now(tz=pytz.UTC)

        # get references to every file starting with "uploads/"
        all_uploads_iterator = self.bucket.list_blobs(prefix='uploads/')
        all_uploads = list(all_uploads_iterator)

        # the oldest one is going to be the "uploads/" folder, so remove that
        upload_times = [u.updated for u in all_uploads]

        try:
            earliest_upload = min(upload_times)
        except ValueError:
            self.logger.error('There may not be any folder with the '
                              'chosen prefix. (By default, "uploads".)')

        uploads_length = len(all_uploads)
        upload_times_length = len(upload_times)
        for upload in all_uploads:
            if upload.updated == earliest_upload:
                all_uploads.remove(upload)
                upload_times.remove(earliest_upload)

        # make sure we only removed one entry
        assert len(all_uploads) == uploads_length - 1
        assert len(upload_times) == upload_times_length - 1

        # keep only the files whose timestamps lie between now and the last
        # iteration of this loop
        self.new_uploads = []
        for upload in all_uploads:
            if upload.updated > self.initial_timestamp:
                self.new_uploads.append(upload)
                self.logger.info('Found new upload: %s', upload)

        # for each of these new uploads, parse necessary information from the
        # filename and write an appropriate entry to Redis
        # Before writing, also check that no hash already in Redis contains
        # this file's name. (We seem to have a problem with double
        # entries in Redis.)
        keys = self.redis_client.keys()
        self.combined_keys = '\t'.join(keys)

        self._write_new_redis_keys()

        # update baseline timestamp
        self.initial_timestamp = soon_to_be_baseline_timestamp

    def _write_new_redis_keys(self):
        for upload in self.new_uploads:
            # verify the upload is a direct upload, and not a web upload
            re_filename = '(uploads(?:/|%2F))(directupload_.+)$'
            try:
                upload_filename = re.search(re_filename, upload.path).group(2)
            except AttributeError as err:
                # this isn't a directly uploaded file
                # or its filename was formatted incorrectly
                self.logger.debug("Failed on filename of %s. Error: %s: %s",
                                  upload.path, type(err).__name__, err)
                continue

            # check for presence of filename in Redis already
            if upload_filename in self.combined_keys:
                self.logger.warning('%s tried to get uploaded a second time.',
                                    upload_filename)
                continue

            # check to see whether this is a special "benchmarking" direct
            # upload
            benchmarking_re = 'benchmarking([0-9]+)special'
            benchmarking_result = re.search(benchmarking_re, upload_filename)
            if benchmarking_result is None:
                # standard direct upload
                self._create_single_redis_entry(
                    upload, upload_filename, upload_filename)
            else:
                # "benchmarking" direct upload
                number_of_images = benchmarking_result.group(1)
                self._create_multiple_redis_entries(
                    upload, upload_filename, number_of_images)

    def _create_multiple_redis_entries(self, upload, upload_filename, number_of_images):
        # make a numbe rof redis entries corresponding to the number found in
        # the benchmarking section of the filename
        for img_num in range(int(number_of_images)):
            current_upload_filename = upload_filename[:-4] + str(img_num) + \
                upload_filename[-4:]
            self._create_single_redis_entry(
                upload, current_upload_filename, upload_filename)

    def _create_single_redis_entry(self, upload, modified_upload_filename,
                                   unmodified_upload_filename):
        # dictionary for uploading to Redis
        field_dict = {}
        field_dict['url'] = upload.public_url
        field_dict['input_file_name'] = "uploads/%s" % unmodified_upload_filename
        field_dict['status'] = "new"
        # filename schema: modelname_modelversion_ppfunc_cuts_etc
        re_fields = 'directupload_([^_]+)_([0-9]+)_([^_]+)_([0-9]+)_.+$'
        fields = re.search(re_fields, unmodified_upload_filename)
        try:
            field_dict['model_name'] = fields.group(1)
            field_dict['model_version'] = fields.group(2)
            field_dict['postprocess_function'] = fields.group(3)
            field_dict['cuts'] = fields.group(4)
        except AttributeError:
            self.logger.warning('Failed on fields of %s.',
                                modified_upload_filename)
            return 0

        field_dict['identity_upload'] = self.HOSTNAME
        field_dict['timestamp_upload'] = time.time() * 1000

        redis_key = 'predict_{}_{}'.format(
            uuid.uuid4().hex,
            modified_upload_filename)

        self.redis_client.hmset(redis_key, field_dict)
        self.logger.debug('Wrote Redis entry of %s for %s.',
                          self.redis_client.hgetall(redis_key), redis_key)
