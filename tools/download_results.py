#!/usr/bin/python
# Copyright 2012 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Export data from HDFS into GS."""



import sys

from cfg import cfg
import common


def main():
  common.setup()
  usage = 'USAGE: {0} hdfs_src gs_dst'.format(common.script_name())
  if len(sys.argv) != 3:
    print usage
    sys.exit(1)

  src = sys.argv[1]
  if sys.argv[2].startswith('gs://') or not sys.argv[2].startswith('/'):
    print usage
    print ('gs_dst should be of the form /path/to/object. gs://{0} will be '
           'prefixed for you.').format(cfg.gs_bucket)
    sys.exit(1)
  dst = 'gs://{0}{1}'.format(cfg.gs_bucket, sys.argv[2])

  common.download(src, dst)

if __name__ == '__main__':
  main()
