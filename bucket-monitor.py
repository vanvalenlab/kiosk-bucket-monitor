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
"""
Watch for uploads into a cloud bucket and an write entry to the Redis
database for each upload.
"""

import logging
import sys

import redis
import decouple

from bucket_monitor import BucketMonitor


def initialize_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console = logging.StreamHandler(stream=sys.stdout)
    console.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Send logs to a file for later inspection.
    file_handler = logging.FileHandler('bucket-monitor.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


if __name__ == '__main__':
    # Get environment variables
    CLOUD_PROVIDER = decouple.config('CLOUD_PROVIDER', default='aws')
    BUCKET_NAME = decouple.config('BUCKET', default='DEFAULT_BUCKET_NAME')
    REDIS_HOST = decouple.config('REDIS_HOST', default='redis-master-0')
    REDIS_PORT = decouple.config('REDIS_PORT', default=6379, cast=int)
    INTERVAL = decouple.config('INTERVAL', default=5, cast=int)
    HOSTNAME = decouple.config('HOSTNAME', default='invalid_hostname')

    # Set up logging
    initialize_logger()

    R = redis.StrictRedis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        charset='utf-8')

    # Create the bucket monitor
    BM = BucketMonitor(
        cloud_provider=CLOUD_PROVIDER,
        bucket_name=BUCKET_NAME,
        redis_client=R,
        hostname=HOSTNAME)

    # Monitor the bucket
    BM.monitor_bucket(INTERVAL)
