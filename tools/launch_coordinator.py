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

"""Create the coordinator's instance to manage the rest of the cluster."""



import subprocess
import time

from cfg import cfg
import common
import gcelib.shortcuts as gce_shortcuts
import util


def main():
  common.setup()

  # CHANGE ME
  zone = 'us-central1-a'
  machtype = 'n1-standard-4-d'
  image = 'projects/google/images/ubuntu-12-04-v20120621'
  # Persistent disk, if any.
  disk = ''
  # If this is set, only this slave will have the disk mounted, and it'll be rw.
  # Otherwise, all slaves get the disk mounted ro
  rw_disk_instance = ''

  print 'Packaging up the stuff the coordinator will need...'
  # tar will insert directories, so flatten the view a bit
  subprocess.call(['cp', 'coordinator/coordinator.py', '.'])
  subprocess.call(['cp', 'coordinator/hadoop_cluster.py', '.'])
  subprocess.call(['tar', 'czf', 'coordinator.tgz', 'hadoop', 'gcelib',
                   'hadoop-tools.jar', 'cfg.py', 'util.py', 'coordinator.py',
                   'hadoop_cluster.py', 'start_setup.sh'])
  subprocess.call(['rm', 'coordinator.py', 'hadoop_cluster.py'])
  # Push to a fixed place for now
  subprocess.call(['gsutil', 'cp', 'coordinator.tgz',
                   cfg.gs_coordinators_tarball])
  subprocess.call(['rm', 'coordinator.tgz'])
  print

  print 'Launching coordinator...'
  util.api.insert_instance(
      name=cfg.coordinator, zone=zone,
      machineType=machtype, image=image,
      serviceAccounts=gce_shortcuts.service_accounts([cfg.compute_scope,
                                                      cfg.rw_storage_scope]),
      networkInterfaces=gce_shortcuts.network(),
      metadata=gce_shortcuts.metadata({
          # Key modified to avoid dots, which are disallowed in v1beta13.
          'startup-script': open('start_setup.sh').read(),
          'bootstrap_sh': open('coordinator/bootstrap.sh').read(),
          'tarball': cfg.gs_coordinators_tarball,
          'gs_bucket': cfg.gs_bucket,
          'zone': zone,
          'machine_type': machtype,
          'image': image,
          'disk': disk,
          'rw_disk_instance': rw_disk_instance,
          'secret': cfg.secret
      }),
      blocking=True
  )
  print

  print 'Waiting for coordinator to come online...'
  while True:
    status, _ = util.get_status(cfg.coordinator)
    print status[1]
    if status == util.InstanceState.SNITCH_READY:
      break
    time.sleep(cfg.poll_delay_secs)
  print

  print 'Controller is ready to receive commands.'

if __name__ == '__main__':
  main()
