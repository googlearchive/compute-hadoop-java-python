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

"""Add slaves to a running Hadoop cluster."""



import sys

import common


def main():
  common.setup()

  if len(sys.argv) != 2:
    print 'USAGE: {0} num_slaves'.format(common.script_name())
    sys.exit(1)

  num_slaves = int(sys.argv[1])

  print 'Adding {0} slaves...'.format(num_slaves)
  common.send_coordinator('/hadoop/add_slaves', {'num_slaves': num_slaves})

if __name__ == '__main__':
  main()
