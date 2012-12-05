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

"""Delete data from HDFS."""



import sys

import common


def main():
  common.setup()
  if len(sys.argv) != 3 or sys.argv[1] != '-f':
    print 'USAGE: {0} -f hdfs_path'.format(common.script_name())
    print 'WARNING: This deletes hdfs_path from your cluster.'
    sys.exit(1)

  nix = sys.argv[2]
  common.send_coordinator('/job/clean', {'path': nix}, verify=True)

if __name__ == '__main__':
  main()
