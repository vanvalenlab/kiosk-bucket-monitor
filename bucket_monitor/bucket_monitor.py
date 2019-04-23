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
import uuid
import datetime
import logging

from google.cloud import storage


class BucketMonitor(object):
    """Watches a bucket for new uploads and adds data for each to Redis."""

    def __init__(self, redis_client, cloud_provider, bucket_name, queue):
        self.redis_client = redis_client
        self.bucket_name = bucket_name
        self.queue = str(queue).lower()
        self.cloud_provider = str(cloud_provider).lower()
        self.logger = logging.getLogger(str(self.__class__.__name__))

        # get initial timestamp to act as a baseline, assume UTC for everything
        self.current_timestamp = datetime.datetime.utcnow()

    def get_storage_api(self):
        if self.cloud_provider == 'gke':
            return storage.Client()
        elif self.cloud_provider == 'aws':
            raise NotImplementedError('{} does not yet support `{}`.'.format(
                self.__class__.__name__, self.cloud_provider))
        else:
            raise ValueError('Invalid value for `cloud_provider`: {}.'.format(
                self.cloud_provider))

    def get_all_uploads(self, prefix=None):
        all_uploads = []
        if self.cloud_provider == 'gke':
            client = self.get_storage_api()
            bucket = client.get_bucket(self.bucket_name)
            all_uploads = bucket.list_blobs(prefix=prefix)
        elif self.cloud_provider == 'aws':
            raise NotImplementedError('{} does not yet support `{}`.'.format(
                self.__class__.__name__, self.cloud_provider))
        return all_uploads

    def scan_bucket_for_new_uploads(self, prefix='uploads/'):
        # get a timestamp to mark the baseline for the next loop iteration
        next_timestamp = datetime.datetime.utcnow()
        self.logger.info('New loop at %s', next_timestamp.isoformat())

        # get references to every file starting with `prefix`
        all_uploads = self.get_all_uploads(prefix=prefix)

        # get current redis keys to avoid double entries
        redis_keys = '\t'.join(self.redis_client.keys())  # TODO: O(n)

        for upload in all_uploads:
            if upload.name == prefix:
                continue  # no need to process the prefix directory

            # only process files uploaded between now and last iteration
            if upload.updated > self.current_timestamp:
                self.logger.info('Found new upload: %s', upload.name)
                # parse necessary information from the filename
                # and write an appropriate entry to Redis
                self.write_new_redis_key(upload, redis_keys)

        self.current_timestamp = next_timestamp  # update baseline timestamp

    def write_new_redis_key(self, upload, redis_keys):
        filename_pattern = '(uploads(?:/|%2F))(directupload_.+)$'
        # verify the upload is a direct upload, and not a web upload
        try:
            re_results = re.search(filename_pattern, upload.path)
            upload_filename = re_results.group(2)
        except AttributeError as err:
            # this isn't a directly uploaded file
            # or its filename was formatted incorrectly
            self.logger.error('Failed on filename of %s. Error: %s: %s',
                              upload.name, type(err).__name__, err)
            return 0

        # check for presence of filename in Redis already
        if upload_filename in redis_keys:
            self.logger.warning('%s tried to get uploaded a second time.',
                                upload_filename)
            return 0

        # is this a special "benchmark" direct upload?
        benchmark_pattern = 'benchmarking([0-9]+)special'
        benchmark_result = re.search(benchmark_pattern, upload_filename)
        if benchmark_result is None:
            # standard direct upload
            self.create_redis_entry(upload, upload_filename, upload_filename)
            return 1

        # "benchmarking" direct upload
        base, ext = os.path.splitext(upload_filename)
        count = int(benchmark_result.group(1))
        for i in range(count):
            new_filename = '{basename}{uid}{ext}'.format(
                basename=base, uid=i, ext=ext)
            self.create_redis_entry(upload, new_filename, upload_filename)
        return count

    def create_redis_entry(self, upload, modified_filename, original_filename):
        """Creates a redis entry based on the `upload_filename`.

        Args:
            upload: object representing item in storage bucket
            modified_filename: string, updated uploaded file name for benchmark
            original_filename: string, name of original uploaded file
            count: int, number of redis entries to create
        """
        # create a unique redis key
        redis_key = '{prefix}_{unique_id}_{filename}'.format(
            prefix='predict',
            unique_id=uuid.uuid4().hex,
            filename=modified_filename)

        # create the new redis key's fields and values
        field_dict = {
            'status': 'new',
            'url': upload.public_url,
            'input_file_name': 'uploads/%s' % original_filename,
            'identity_upload': os.getenv('HOSTNAME'),
            'created_at': datetime.datetime.utcnow().isoformat(),
            'updated_at': datetime.datetime.utcnow().isoformat()
        }

        try:
            # filename schema: modelname_modelversion_ppfunc_cuts_etc
            pattern = 'directupload_([^_]+)_([0-9]+)_([^_]+)_([0-9]+)_.+$'
            fields = re.search(pattern, original_filename)
            field_dict.update({
                'model_name': fields.group(1),
                'model_version': fields.group(2),
                'postprocess_function': fields.group(3),
                'cuts': fields.group(4)
            })
        except AttributeError:
            self.logger.error('Failed to parse fields from filename: `%s`.',
                              original_filename)
            return False

        self.redis_client.hmset(redis_key, field_dict)
        self.redis_client.lpush(self.queue, redis_key)
        self.logger.debug('Wrote Redis entry of %s for %s.',
                          self.redis_client.hgetall(redis_key), redis_key)
        return True
