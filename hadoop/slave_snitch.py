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

"""Lets the snitch control hadoop slave."""



import logging
import subprocess

import bottle
from cfg import cfg

import common_snitch


def main():
  app = bottle.Bottle()

  @app.post('/start')
  def start_slave():
    common_snitch.authorize()
    # Just start the two components that run on us. start-mapred.sh and
    # start-dfs.sh just ssh in and do this anyway
    logging.info('Starting datanode...')
    subprocess.call([cfg.hadoop_bin + 'hadoop-daemon.sh', 'start', 'datanode'])
    logging.info('Starting tasktracker...')
    subprocess.call([cfg.hadoop_bin + 'hadoop-daemon.sh', 'start',
                     'tasktracker'])
    logging.info('Both started!')
    return cfg.ok_reply

  common_snitch.start_snitch(app)


if __name__ == '__main__':
  main()
