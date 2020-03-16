# kiosk-bucket-monitor

[![Build Status](https://travis-ci.com/vanvalenlab/kiosk-bucket-monitor.svg?branch=master)](https://travis-ci.com/vanvalenlab/kiosk-bucket-monitor)
[![Coverage Status](https://coveralls.io/repos/github/vanvalenlab/kiosk-bucket-monitor/badge.svg?branch=master)](https://coveralls.io/github/vanvalenlab/kiosk-bucket-monitor?branch=master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/vanvalenlab/kiosk-bucket-monitor/blob/master/LICENSE)

The `StaleFileBucketMonitor` is used to remove files from the DeepCell Kiosk cloud storage bucket. This helps to save costs by preventing files from being stored in the bucket indefinitely.

This repository is part of the [DeepCell Kiosk](https://github.com/vanvalenlab/kiosk-console). More information about the Kiosk project is available through [Read the Docs](https://deepcell-kiosk.readthedocs.io/en/master) and our [FAQ](http://www.deepcell.org/faq) page.

## Configuration

The bucket monitor is configured using environment variables. Please find a table of all environment variables and their descriptions below.

| Name | Description | Default Value |
| :--- | :--- | :--- |
| `BUCKET` | **REQUIRED**: The name of the bucket to monitor. |  |
| `AGE_THRESHOLD` | Files are removed if they are older than this many seconds. | `259200` |
| `CLOUD_PROVIDER` | The cloud provider hosting the DeepCell Kiosk | `gke` |
| `INTERVAL` | How frequently the bucket is monitored, in seconds. | `21600` |
| `PREFIXES` | A comma separated string of the bucket's directories to monitor. | `uploads/,output/` |

## Contribute

We welcome contributions to the [kiosk](https://github.com/vanvalenlab/kiosk-console) and its associated projects. If you are interested, please refer to our [Developer Documentation](https://deepcell-kiosk.readthedocs.io/en/master/DEVELOPER.html), [Code of Conduct](https://github.com/vanvalenlab/kiosk-console/blob/master/CODE_OF_CONDUCT.md) and [Contributing Guidelines](https://github.com/vanvalenlab/kiosk-console/blob/master/CONTRIBUTING.md).

## License

This software is license under a modified Apache-2.0 license. See [LICENSE](/LICENSE) for full  details.

## Copyright

Copyright © 2018-2020 [The Van Valen Lab](http://www.vanvalen.caltech.edu/) at the California Institute of Technology (Caltech), with support from the Paul Allen Family Foundation, Google, & National Institutes of Health (NIH) under Grant U24CA224309-01.
All rights reserved.
