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

"""Config shared between all instances and local tools."""

import json
import httplib2

# These constants are used by the Config object

COORDINATOR = 'coordinator'
HADOOP_NAMENODE = 'hadoop-namenode'
HADOOP_JOBTRACKER = 'hadoop-jobtracker'
PORT = 8888  # The port of both coordinator and snitches
NUM_WORKERS = 20  # Depends on request quota/second
METADATA = 'http://metadata/0.1/meta-data/'
EDISK_LOCATION = '/mnt/hadoop'


class Config(object):
  """Singleton that stores config."""

  def __init__(self):
    # General communication

    self.port = PORT
    self.ok_reply = json.dumps({'result': 'ok'})
    self.secret = ''
    # For instance-to-instance calls, don't need to use util.name_to_ip()
    self.ip_via_api = True

    # General

    self.poll_delay_secs = 2.0
    self.project_id = None

    # Instance names

    self.coordinator = COORDINATOR
    self.hadoop_namenode = HADOOP_NAMENODE
    self.hadoop_jobtracker = HADOOP_JOBTRACKER

    # Instance creation

    self.zone = None
    self.machine_type = None
    self.image = None
    self.disk = None
    self.rw_disk_instance = None
    self.external_ips = True
    scope_base = 'https://www.googleapis.com/auth/'
    self.rw_storage_scope = scope_base + 'devstorage.read_write'
    self.ro_storage_scope = scope_base + 'devstorage.read_only'
    self.compute_scope = scope_base + 'compute'
    self.download_attempts = 3
    self.num_workers = NUM_WORKERS

    # Hadoop details

    self.hadoop_url = 'archive.apache.org/dist/hadoop/common'
    # Use latest stable version of Hadoop, as of 2/4/2013.
    self.hadoop_version = '1.1.1'
    self.hadoop_fn = 'hadoop-{0}'.format(self.hadoop_version)
    self.hadoop_bin = '/home/hadoop/hadoop/bin/'
    # This is where ephemeral disk gets mounted. Note this location is hardcoded
    # in a few places (the hadoop config, mainly)
    self.edisk_location = EDISK_LOCATION
    # Depends on hdfs replication value
    self.needed_slaves = 3

    # Google Storage locations

    self.gs_bucket = None
    self.gs_hadoop_conf = None
    self.gs_hadoop_tarball = None
    self.gs_coordinators_tarball = None
    self.gs_snitch_tarball = None
    self.gs_tools_jar = None

  def update_from_metadata(self):
    """Update by querying the metadata server. Only works on instances."""

    # This method is only called on instances, meaning we don't need the API to
    # lookup an external IP address
    self.ip_via_api = False

    def get_md(key, base=METADATA + 'attributes/'):
      return httplib2.Http().request(base + key, 'GET')[1]

    self.project_id = get_md('project-id', base=METADATA)
    self.secret = get_md('secret')
    self.zone = get_md('zone')
    self.machine_type = get_md('machine_type')
    self.image = get_md('image')
    self.disk = get_md('disk')
    self.rw_disk_instance = get_md('rw_disk_instance')
    self.set_bucket(get_md('gs_bucket'))

  def set_bucket(self, bucket):
    """Set the GS bucket, and update config URLs involving GS."""
    self.gs_bucket = bucket
    url = 'gs://{0}/'.format(bucket)
    self.gs_hadoop_conf = url + 'hadoop_conf.tgz'
    self.gs_hadoop_tarball = url + self.hadoop_fn + '.tar.gz'
    self.gs_coordinators_tarball = url + 'coordinator-tarball.tgz'
    self.gs_snitch_tarball = url + 'snitch-tarball.tgz'
    self.gs_tools_jar = url + 'hadoop-tools.jar'

cfg = Config()
