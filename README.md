# kiosk-bucket-monitor

[![Build Status](https://travis-ci.com/vanvalenlab/kiosk-bucket-monitor.svg?branch=master)](https://travis-ci.com/vanvalenlab/kiosk-bucket-monitor)
[![Coverage Status](https://coveralls.io/repos/github/vanvalenlab/kiosk-bucket-monitor/badge.svg?branch=master)](https://coveralls.io/github/vanvalenlab/kiosk-bucket-monitor?branch=master)

The `StaleFileBucketMonitor` is used to remove files from the DeepCell Kiosk cloud storage bucket. This helps to save costs by preventing files from being stored in the bucket indefinitely.

## Configuration

The bucket monitor is configured using environment variables. Please find a table of all environment variables and their descriptions below.

| Name | Description | Default Value |
| :---: | :---: | :---: |
| `BUCKET` | **REQUIRED**: The name of the bucket to monitor. |  |
| `AGE_THRESHOLD` | Files are removed if they are older than this many seconds. | `259200` |
| `CLOUD_PROVIDER` | The cloud provider hosting the DeepCell Kiosk | `gke` |
| `INTERVAL` | How frequently the bucket is monitored, in seconds. | `21600` |
| `PREFIXES` | A comma separated string of the bucket's directories to monitor. | `uploads/,output/` |
