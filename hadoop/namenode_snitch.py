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

"""Lets the coordinator control the NameNode."""



import logging
import multiprocessing
import subprocess

import bottle
from cfg import cfg
import util

import common_snitch


def send_update(operation, msg):
  logging.info('State of %s: %s', operation, msg)
  data = {'operation': operation, 'state': msg}
  util.talk_to_agent(cfg.coordinator, '/node/op_status', data)


def do_transfer(operation, src, dst):
  send_update(operation, 'Starting copy {0} -> {1}'.format(src, dst))
  subprocess.call(['java', '-cp', 'hadoop-tools.jar', 'com.google.GsHdfs', src,
                   dst, operation])


def main():
  app = bottle.Bottle()

  @app.post('/transfer')
  def transfer():
    common_snitch.authorize()
    operation = bottle.request.forms.get('operation')
    src = bottle.request.forms.get('src')
    dst = bottle.request.forms.get('dst')
    multiprocessing.Process(target=do_transfer, args=(operation, src,
                                                      dst)).start()
    # We'll send info later if there are problems
    return cfg.ok_reply

  @app.post('/clean')
  def clean():
    common_snitch.authorize()
    path = bottle.request.forms.get('path')
    subprocess.call([cfg.hadoop_bin + 'hadoop', 'fs', '-rmr', path])
    return cfg.ok_reply

  common_snitch.start_snitch(app)

if __name__ == '__main__':
  main()
