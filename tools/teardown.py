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

"""A script to teardown the cluster."""



import logging
import sys
import time

from cfg import cfg
import common
import util


def main():
  common.setup()
  run = raw_input('Really delete all your instances? [y/n] ')
  if run != 'y':
    print 'Never mind.'
    sys.exit()

  def nix(instance):
    logging.info('Shutting down %s', instance)
    util.api.delete_instance(instance=instance, blocking=True)

  sched = util.Scheduler(cfg.num_workers * 2)
  for name in util.get_instance_names():
    sched.schedule(nix, (name,))

  # Block
  left = len(util.get_instance_names())
  while left:
    logging.info('%s instances still left', left)
    time.sleep(cfg.poll_delay_secs)
    left = len(util.get_instance_names())
  print

  print 'All gone!'

if __name__ == '__main__':
  main()
