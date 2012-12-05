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

"""Set up hadoop on masters or slaves."""



import logging
import os
import socket
import subprocess

from cfg import cfg
import util


def report_fail(msg):
  hostname = socket.gethostname()
  data = {'name': hostname, 'msg': msg}
  # Since we're running in an instance, DNS will do the IP lookup for us
  util.talk_to_agent(cfg.coordinator, '/instance/report_fail', data)


def setup():
  """Installs Hadoop and dependencies and imports configuration."""
  hostname = socket.gethostname()
  is_namenode = hostname == cfg.hadoop_namenode

  # Set up directories for Hadoop
  subprocess.check_call(['mkdir', '-p', cfg.edisk_location + '/dfs'])
  subprocess.check_call(['mkdir', '-p', cfg.edisk_location + '/mapred'])

  # Install prereqs
  util.retry_call(['sudo', 'apt-get', 'install', '-y', 'openjdk-6-jre-headless',
                   'python-cherrypy3', 'python-openssl'], report_fail)

  # Grab and unpack hadoop
  # Mirroring Hadoop on GS avoids hitting Apache mirrors repeatedly
  util.retry_call(['gsutil', 'cp', cfg.gs_hadoop_tarball,
                   cfg.hadoop_fn + '.tar.gz'], report_fail)
  subprocess.check_call(['tar', 'xzf', cfg.hadoop_fn + '.tar.gz'])
  # Be convenient
  subprocess.check_call(['mv', cfg.hadoop_fn, 'hadoop'])

  # Pull in our own config, overwriting some default files
  util.retry_call(['gsutil', 'cp', cfg.gs_hadoop_conf, 'hadoop_conf.tgz'],
                  report_fail)
  subprocess.check_call(['tar', 'xzf', 'hadoop_conf.tgz'])

  if is_namenode:
    logging.info('Setting up namenode...')
    # Grab a jar
    util.retry_call(['gsutil', 'cp', cfg.gs_tools_jar, 'hadoop-tools.jar'])
    # Initialize the filesystem
    subprocess.check_call([cfg.hadoop_bin + 'hadoop', 'namenode', '-format'])
    # Launch
    subprocess.check_call([cfg.hadoop_bin + 'hadoop-daemon.sh', 'start',
                           'namenode'])
    logging.info('Namenode ready!')


def main():
  cfg.update_from_metadata()
  state = 'READY'
  try:
    setup()
  except subprocess.CalledProcessError as e:
    logging.error('Setup failed: %s', str(e))
    state = 'FAILED'
  os.execl('/home/hadoop/snitch.py', '/home/hadoop/snitch.py', state)

if __name__ == '__main__':
  main()
