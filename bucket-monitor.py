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

from google.cloud import storage
from redis import StrictRedis
from redis.exceptions import ConnectionError

def main():
    '''
    Watch a cloud bucket and write an entry to Redis for each upload.
    '''
    # read in environment variables
    CLOUD_PROVIDER = os.environ['CLOUD_PROVIDER']
    BUCKET_NAME = os.environ['BUCKET']
    REDIS_HOST = os.environ['REDIS_HOST']
    REDIS_PORT = os.environ['REDIS_PORT']
    INTERVAL = os.environ['INTERVAL']

    # confiugre logger
    bm_logger = logging.getLogger('bucket-monitor')
    bm_logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('bucket-monitor.log')
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    bm_logger.addHandler(fh)

    # establish cloud connection
    if CLOUD_PROVIDER=="gke":
        client = storage.Client()
        bucket = client.get_bucket(BUCKET_NAME)

    # establish Redis connection
    r = StrictRedis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        charset='utf-8')

    # get initial timestamp to act as a baseline, assuming UTC for everything
    initial_timestamp = datetime.datetime.now(tz=pytz.UTC)

    # enter main loop
    while True:
        # logging loop beginning
        bm_logger.debug("New loop at " + str(initial_timestamp))
        
        # get a timestamp to mark the baseline for the next loop iteration
        soon_to_be_baseline_timestamp = datetime.datetime.now(tz=pytz.UTC)
        
        # get references to every file starting with "uploads/"
        all_uploads_iterator = bucket.list_blobs(prefix='uploads/')
        all_uploads = list(all_uploads_iterator)

        # the oldest one is going to be the "uploads/" folder, so remove that
        upload_times = []
        for upload in all_uploads:
            upload_times.append(upload.updated)
        try:
            earliest_upload = min(upload_times)
        except ValueError:
            bm_logger.error("There may not be any folder with the " +
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
        new_uploads = []
        for upload in all_uploads:
            if upload.updated > initial_timestamp:
                new_uploads.append(upload)

        # for each of these new files, parse necessary information from the
        # filename and write an appropriate entry to Redis
        # Before writing, also check that no hash already in Redis contains
        # this file's name. (We seem to have a problem with double
        # entries in Redis.)
        while True:
            try:
                all_keys = r.keys()
                break
            except ConnectionError:
                # For some reason, we're unable to connect to Redis right now.
                # Keep trying until we can.
                bm_logger.warn("Trouble connecting to Redis. Retrying.")
                time.sleep(5)
        combined_keys = '\t'.join(all_keys) # long string containing all keys
        for upload in new_uploads:
            re_filename = '(uploads(?:/|%2F))(directupload_.+)$'
            try:
                upload_filename = re.search(re_filename, upload.path).group(2)
            except AttributeError:
                # this isn't a directly uploaded file
                # or its filename was formatted incorrectly
                bm_logger.debug("Failed on filename of " +
                        str(upload.path) + ".")
                continue
            # check for presence of filename in Redis already
            if upload_filename in combined_keys:
                bm_logger.debug(upload_filename +
                        " tried to get uploaded a second time.")
                continue
            # dictionary for uploading to Redis
            field_dict = {}
            field_dict['url'] = upload.public_url
            field_dict['file_name'] = "uploads/" + upload_filename
            field_dict['status'] = "new"
            # filename schema: modelname_modelversion_ppfunc_cuts_etc
            re_fields = 'directupload_([^_]+)_([0-9]+)_([^_]+)_([0-9]+)_.+$'
            fields = re.search(re_fields, upload_filename)
            try:
                field_dict['model_name'] = fields.group(1)
                field_dict['model_version'] = fields.group(2)
                field_dict['postprocessing_function'] = fields.group(3)
                field_dict['cuts'] = fields.group(4)
            except AttributeError:
                bm_logger.debug("Failed on fields of " +
                        str(upload_filename) + ".")
                continue
            field_dict['timestamp_upload'] = time.time() * 1000
            redis_key = "predict_" + uuid.uuid4().hex + \
                    "_" + upload_filename
            while True:
                try:
                    r.hmset(redis_key, field_dict)
                    break
                except ConnectionError:
                    # For some reason, we're unable to connect to Redis
                    # right now. Keep trying until we can.
                    bm_logger.warn("Trouble connecting to Redis. Retrying.")
                    time.sleep(5)
            bm_logger.debug("Wrote Redis entry for " + upload_filename + ".")

        # update baseline timestamp and sleep
        initial_timestamp = soon_to_be_baseline_timestamp
        time.sleep( int(INTERVAL) )

if __name__=='__main__':
    main()
