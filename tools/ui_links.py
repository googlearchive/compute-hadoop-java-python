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

"""Grab links to the Hadoop web UI."""



import os.path
import socket
import subprocess

from cfg import cfg
import common
import util

# These are your gcutils keys, used to authenticate to your instances. Run
# 'gcutil auth' if you have not used gcutil yet
SSH_KEYS = os.path.expanduser('~/.ssh/google_compute_engine')


def next_free_port():
  sock = socket.socket()
  sock.bind(('', 0))
  return sock.getsockname()[1]


def setup_tunnel(ip, port):
  """Returns the local port to which ip:port is bound."""
  local_port = next_free_port()
  cmd = ['ssh',
         # Authenticate to the instance using the user's public keys
         '-i', SSH_KEYS,
         # Don't check the host's public key, since we don't have it
         '-o', 'StrictHostKeyChecking=no',
         # Background ssh after connecting, so it's just a tunnel
         '-f',
         ip,
         # Forward local_port to ip:port
         '-L', '{0}:127.0.0.1:{1}'.format(local_port, port),
         # Don't execute a remote command
         '-N'
        ]
  print 'Executing: {0}'.format(' '.join(cmd))
  subprocess.call(cmd)
  return local_port


def main():
  common.setup()
  jobtracker_port = setup_tunnel(util.name_to_ip(cfg.hadoop_jobtracker), 50030)
  namenode_port = setup_tunnel(util.name_to_ip(cfg.hadoop_namenode), 50070)
  print
  print ('ssh tunnels are running in the background to provide access to the '
         'Hadoop web interface. You can close the tunnels by killing the '
         'ssh process responsible. "ps aux | grep ssh" should list processes '
         'matching the commands indicated above.')
  print
  print '*** JobTracker: http://localhost:{0}'.format(jobtracker_port)
  print '*** NameNode: http://localhost:{0}'.format(namenode_port)

if __name__ == '__main__':
  main()
