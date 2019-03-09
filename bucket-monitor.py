# bucket-monitor.py
# Watch for uploads into a cloud bucket and an write entry to the Redis
# database for each upload.

import os
import datetime
import pytz
import re
import time
import uuid
import logging
import sys

from google.cloud import storage
from redis import StrictRedis
from redis.exceptions import ConnectionError

class BucketMonitor():
    '''
    Watch a cloud bucket and write an entry to Redis for each upload.
    '''
    def __init__(self):
        # read in environment variables
        self.CLOUD_PROVIDER = os.environ['CLOUD_PROVIDER']
        self.BUCKET_NAME = os.environ['BUCKET']
        self.REDIS_HOST = os.environ['REDIS_HOST']
        self.REDIS_PORT = os.environ['REDIS_PORT']
        self.INTERVAL = os.environ['INTERVAL']

        # confiugre logger
        self._configure_logger()

        # establish cloud connection
        if self.CLOUD_PROVIDER=="gke":
            self.bucket_client = storage.Client()
            self.bucket = self.bucket_client.get_bucket(self.BUCKET_NAME)

        # establish Redis connection
        self.r = StrictRedis(
            host=self.REDIS_HOST,
            port=self.REDIS_PORT,
            decode_responses=True,
            charset='utf-8')

        # get initial timestamp to act as a baseline, assuming UTC for everything
        self.initial_timestamp = datetime.datetime.now(tz=pytz.UTC)

    def _configure_logger(self):
        self.bm_logger = logging.getLogger('bucket-monitor')
        self.bm_logger.setLevel(logging.DEBUG)
        # Send logs to stdout so they can be read via Kubernetes.
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.INFO)
        formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        sh.setFormatter(formatter)
        self.bm_logger.addHandler(sh)
        # Also send logs to a file for later inspection.
        fh = logging.FileHandler('bucket-monitor.log')
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.bm_logger.addHandler(fh)

    def monitor_bucket(self):
        # scan for new bucket uploads, write corresponding redis entires, and
        # then sleep
        while True:
            self.scan_bucket_for_new_uploads()
            time.sleep( int(self.INTERVAL) )

    def scan_bucket_for_new_uploads(self):
        # logging loop beginning
        self.bm_logger.info("New loop at " + str(self.initial_timestamp))
        
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
            self.bm_logger.error("There may not be any folder with the " +
                    "chosen prefix. (By default, \"uploads\".)")
        uploads_length = len(all_uploads)
        upload_times_length = len(upload_times)
        for upload in all_uploads:
            if upload.updated == earliest_upload:
                all_uploads.remove(upload)
                upload_times.remove(earliest_upload)
        assert len(all_uploads)==(uploads_length-1)
        assert len(upload_times)==(upload_times_length-1)

        # keep only the files whose timestamps lie between now and the last
        # iteration of this loop
        self.new_uploads = []
        for upload in all_uploads:
            if upload.updated > self.initial_timestamp:
                self.new_uploads.append(upload)
                self.bm_logger.info("Found new upload: " + str(upload))
        
        # for each of these new uploads, parse necessary information from the
        # filename and write an appropriate entry to Redis
        # Before writing, also check that no hash already in Redis contains
        # this file's name. (We seem to have a problem with double
        # entries in Redis.)
        self._get_all_redis_keys()
        self._write_new_redis_keys()

        # update baseline timestamp
        self.initial_timestamp = soon_to_be_baseline_timestamp

    def _get_all_redis_keys(self):
        while True:
            try:
                all_keys = self.r.keys()
                break
            except ConnectionError as err:
                # For some reason, we're unable to connect to Redis right now.
                # Keep trying until we can.
                self.bm_logger.warn("Trouble connecting to Redis. " + 
                    str(type(err).__name__) + ": " + str(err) + " \nRetrying.")
                time.sleep(5)
        # long string containing all keys
        self.combined_keys = '\t'.join(all_keys)

    def _write_new_redis_keys(self):
        for upload in self.new_uploads:
            # verify that we're dealing with a direct upload, and not a web
            # upload
            re_filename = '(uploads(?:/|%2F))(directupload_.+)$'
            try:
                upload_filename = re.search(re_filename, upload.path).group(2)
            except AttributeError as err:
                # this isn't a directly uploaded file
                # or its filename was formatted incorrectly
                self.bm_logger.debug("Failed on filename of " +
                        str(upload.path) + ". Error %s: %s",
                        type(err).__name__, err )
                continue

            # check for presence of filename in Redis already
            if upload_filename in self.combined_keys:
                self.bm_logger.warn(upload_filename +
                        " tried to get uploaded a second time.")
                continue

            # check to see whether this is a special "benchmarking" direct
            # upload
            benchmarking_re = 'benchmarking([0-9]+)special'
            benchmarking_result = re.search(benchmarking_re, upload_filename)
            if benchmarking_result is None:
                # standard direct upload
                self._create_single_redis_entry(upload, upload_filename,
                        upload_filename)
            else:
                # "benchmarking" direct upload
                number_of_images = benchmarking_result.group(1)
                self._create_multiple_redis_entries(upload, upload_filename,
                        number_of_images)

    def _create_multiple_redis_entries(self, upload, upload_filename, 
            number_of_images):
        # make a numbe rof redis entries corresponding to the number found in
        # the benchmarking section of the filename
        for img_num in range(int(number_of_images)):
            current_upload_filename = upload_filename[:-4] + str(img_num) + \
                    upload_filename[-4:]
            self._create_single_redis_entry(upload, current_upload_filename, 
                    upload_filename)

    def _create_single_redis_entry(self, upload, modified_upload_filename,
            unmodified_upload_filename):
        # dictionary for uploading to Redis
        field_dict = {}
        field_dict['url'] = upload.public_url
        field_dict['file_name'] = "uploads/" + unmodified_upload_filename
        field_dict['status'] = "new"
        # filename schema: modelname_modelversion_ppfunc_cuts_etc
        re_fields = 'directupload_([^_]+)_([0-9]+)_([^_]+)_([0-9]+)_.+$'
        fields = re.search(re_fields, unmodified_upload_filename)
        try:
            field_dict['model_name'] = fields.group(1)
            field_dict['model_version'] = fields.group(2)
            field_dict['postprocessing_function'] = fields.group(3)
            field_dict['cuts'] = fields.group(4)
        except AttributeError:
            self.bm_logger.warn("Failed on fields of " +
                    str(modified_upload_filename) + ".")
            return 0

        field_dict['timestamp_upload'] = time.time() * 1000
        redis_key = "predict_" + uuid.uuid4().hex + \
                "_" + modified_upload_filename
        self._write_redis_entry(redis_key, field_dict, 
                modified_upload_filename)

    def _write_redis_entry(self, redis_key, field_dict, upload_filename):
        while True:
            try:
                self.r.hmset(redis_key, field_dict)
                break
            except ConnectionError as err:
                # For some reason, we're unable to connect to Redis
                # right now. Keep trying until we can.
                self.bm_logger.warn("Trouble connecting to Redis. Retrying." +
                        " %s: %s", type(err).__name__, err)
                time.sleep(5)
        self.bm_logger.debug("Wrote Redis entry for " + upload_filename + ".")

if __name__=='__main__':
    bm = BucketMonitor()
    bm.monitor_bucket()
