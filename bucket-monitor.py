# bucket-monitor.py
# Watch for uploads into a cloud bucket and an write entry to the Redis databse
# for each upload.

from google.cloud import storage
from redis import StrictRedis
import pdb

import os
import datetime
import pytz
import re
import time

def main():
    '''
    Watch a cloud bucket and write an entry to Redis for each upload.
    '''
    # read in environment variables
    CLOUD_PROVIDER = os.environ['CLOUD_PROVIDER']
    if CLOUD_PROVIDER=="gke":
        BUCKET_NAME = os.environ['GKE_BUCKET']
    REDIS_HOST = os.environ['REDIS_HOST']
    REDIS_PORT = os.environ['REDIS_PORT']

    # establish cloud connection
    if CLOUD_PROVIDER=="gke":
        client = storage.Client()
        bucket = client.get_bucket(BUCKET_NAME)

    # establish Redis connection
    redis = StrictRedis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        charset='utf-8')

    # get initial timestamp to act as a baseline, assuming UTC for everything
    initial_timestamp = datetime.datetime.now(tz=pytz.UTC)

    # enter main loop
    while True:
        # ge ta timestamp to mark the baseline for the next loop iteration
        soon_to_be_baseline_timestamp = datetime.datetime.now(tz=pytz.UTC)
        
        # get references to every file starting with "uploads/"
        all_uploads_iterator = bucket.list_blobs(prefix='uploads/')
        all_uploads = list(all_uploads_iterator)

        # the oldest one is going to be the "uploads/" folder, so remove that
        upload_times = []
        for upload in all_uploads:
            upload_times.append(upload.updated)
        earliest_upload = min(upload_times)
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
        for upload in all_uploads:
            if upload.updated < initial_timestamp:
                all_uploads.remove(upload)

        # for each of these files, parse necessary information from the
        # filename and write an appropriate entry to Redis
        for upload in all_uploads:
            re_filename = '(uploads(?:/|%2F)directupload_)(.+)$'
            upload_filename = re.search(re_string, upload.path).group(2)
            if upload_filename==None:
                # this isn't a directly uploaded file
                # or its filenmae was formatted incorrectly
                continue
                #raise TypeError("It looks like the path of the uploaded " +
                #        "file is formatted without a match to " re_string)
            # filename schema: modelname_modelversion_ppfunc_cuts_etc
            re_fields = '([^_]+)_([0-9]+)_([^_]+)_([0-9]+)_.+'
            fields = re.search(re_fields, upload_filename)
            model_name = re_fields.group(1)
            model_version = re_fields.group(2)
            postprocessing_function = re_fields.group(3)
            cuts = re_fields.group(4)
            redis.hmset( upload_filename + "_" + uuid.uuid4().hex,
                    'url', upload.public_url,
                    'model_name', model_name,
                    'model_version', model_version,
                    'file_name', 'uploads/' + upload_filename,
                    'postprocess_function', postprocessing_function,
                    'cuts', cuts,
                    'status', 'new'
                    )

        # update baseline timestamp and sleep
        initial_timestamp = soon_to_be_baseline_timestamp
        time.sleep(3)

    pdb.set_trace()



if __name__=='__main__':
    main()
