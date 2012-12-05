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

"""Run a TeraSort MapReduce job.

Phase 1 generates 1TB of data within your cluster.
Phase 2 sorts the data.
Phase 3 verifies the sort was correct.
"""



import os.path
import sys

from cfg import cfg
import common


def main():
  common.setup()
  usage = ('USAGE: {0} [1,2,3]\nPhase 1 generates data, phase 2 sorts it, and '
           'phase 3 validates it.'.format(common.script_name()))

  if len(sys.argv) != 2:
    print usage
    sys.exit(1)

  # Detect if the user has the jar
  jar = 'hadoop-examples-{0}.jar'.format(cfg.hadoop_version)
  if not os.path.exists(jar):
    print ('You need {0}, which contains the Terasort MapReduce job, in your '
           'current directory.').format(jar)
    print ('Please run the following commands to get it, then re-run this '
           'script.')
    print
    tarball = '{0}.tar.gz'.format(cfg.hadoop_fn)
    print 'wget http://{0}/{1}/{2}'.format(cfg.hadoop_url, cfg.hadoop_fn,
                                           tarball)
    print 'tar xzf {0}'.format(tarball)
    print 'cp {0}/{1} .'.format(cfg.hadoop_fn, jar)
    sys.exit(1)

  # TODO Figure out number of tasks programatically. The defaults are sometimes
  # 1.
  num_tasks = 100
  phase = sys.argv[1]

  job_args = []
  if phase == '1':
    gigabytes = 1000
    # Convert GB->bytes, then divide by 100
    hundred_bytes = gigabytes * (10 ** 7)

    job_args = ['teragen', '-Dmapred.map.tasks={0}'.format(num_tasks),
                hundred_bytes, '/job_input/terasort']
  elif phase == '2':
    # The terasort driver automatically uses as many map tasks as possible.
    job_args = ['terasort', '-Dmapred.reduce.tasks={0}'.format(num_tasks),
                '/job_input/terasort', '/job_output/terasort']
  elif phase == '3':
    job_args = ['teravalidate', '/job_output/terasort',
                '/job_output/teravalidate']
  else:
    print usage
    sys.exit(1)

  common.start_job(jar, job_args)

if __name__ == '__main__':
  main()
