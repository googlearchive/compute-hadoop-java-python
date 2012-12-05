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

"""Lets the coordinator control the JobTracker."""



import json
import logging
import os.path
import subprocess
import tempfile
import urlparse
import uuid

import bottle
from cfg import cfg
import util

import common_snitch


def get_file(src, local_dst):
  """Download src from the web or GS to local_dst."""
  if urlparse.urlparse(src).scheme == 'gs':
    subprocess.call(['gsutil', 'cp', src, local_dst])
  else:
    subprocess.call(['wget', src, '-O', local_dst])


def main():
  app = bottle.Bottle()
  tempfile.tempdir = cfg.edisk_location

  @app.post('/start')
  def start_jobtracker():
    common_snitch.authorize()
    subprocess.call([cfg.hadoop_bin + 'hadoop-daemon.sh', 'start',
                     'jobtracker'])
    logging.info('Start done!')
    return cfg.ok_reply

  @app.post('/job/start')
  def start_job():
    """Downloads a JAR locally and submits a MapReduce job."""
    common_snitch.authorize()
    jar = bottle.request.forms.get('jar')
    job_args = map(str, json.loads(bottle.request.forms.get('args')))
    local_jobdir = tempfile.mkdtemp()
    local_jar = os.path.join(local_jobdir, os.path.basename(jar))

    get_file(jar, local_jar)

    job_name = '{0}_{1}'.format(local_jar, uuid.uuid1())
    util.bg_exec([cfg.hadoop_bin + 'hadoop', 'jar', local_jar] + job_args,
                 '/home/hadoop/log_job_{0}'.format(os.path.basename(job_name)))
    return cfg.ok_reply

  common_snitch.start_snitch(app)

if __name__ == '__main__':
  main()
