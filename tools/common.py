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

"""Shared code for local tools."""



import json
import os
import pprint
import subprocess
import sys
import textwrap
import time
import urlparse
import uuid

from cfg import cfg
import util


def setup():
  # CHANGE ME
  cfg.set_bucket('GS-bucket')
  cfg.project_id = 'compute-project-name'

  if os.path.exists('secret'):
    cfg.secret = open('secret').read().rstrip()
  if not cfg.secret:
    print 'Run tools/gen_passwd.py first to generate a password.'
    sys.exit(1)
  util.setup_api(service_account=False)


def send_coordinator(cmd, data, verify=False):
  data['secret'] = cfg.secret
  if verify:
    return util.checked_do(cfg.coordinator, cmd, data=data)
  else:
    return util.talk_to_agent(util.name_to_ip(cfg.coordinator), cmd, data=data)


def put_file(uri):
  """Uploads uri to GS if it's a local file. Returns (filename, is_gs)."""
  if urlparse.urlparse(uri).scheme:
    return (uri, False)
  else:
    # It's local, let's push first
    gs_fn = 'gs://{0}/tmp_hadoop/{1}/{2}'.format(cfg.gs_bucket, uuid.uuid1(),
                                                 os.path.basename(uri))
    subprocess.call(['gsutil', 'cp', uri, gs_fn])
    return (gs_fn, True)


def poll_operation(op):
  """Blocks until a transfer operation is done."""
  url = '/status/op/{0}'.format(op)
  print 'Polling...'
  while True:
    resp = send_coordinator(url, {})
    print resp['state']
    if resp['state'] == 'Done':
      break
    time.sleep(cfg.poll_delay_secs)
  print


def upload(uri, hdfs_input=None):
  """Blockingly sends a file to the coordinator to import into HDFS."""
  if hdfs_input is None:
    hdfs_input = hdfs_for_upload(uri)
  print 'Uploading input...'
  src, is_gs = put_file(uri)
  result = send_coordinator('/transfer', {'src': src, 'dst': hdfs_input},
                            verify=True)
  poll_operation(result['operation'])
  # Clean up GS
  if is_gs:
    subprocess.call(['gsutil', 'rm', src])
  return hdfs_input


def download(src, dst):
  """Blockingly transfers a file from HDFS to GS."""
  result = send_coordinator('/transfer', {'src': src, 'dst': dst},
                            verify=True)
  poll_operation(result['operation'])
  print 'gsutil ls {0}'.format(dst)
  return dst


def hdfs_for_upload(uri):
  return os.path.join('/job_input', str(uuid.uuid1()), os.path.basename(uri))


def start_job(jar_uri, job_args):
  print 'Starting job...'
  jar, is_gs = put_file(jar_uri)
  send_coordinator('/job/submit', {'jar': jar, 'job_args':
                                   json.dumps(job_args)}, verify=True)
  # Clean up GS
  if is_gs:
    subprocess.call(['gsutil', 'rm', jar])
  print 'Submitted!'
  return True


def pprint_status(data):
  """Pretty-prints the data from the /status/cluster call."""
  print '=== Hadoop data ({0} seconds old) ==='.format(data['hadoop_staleness'])
  pprint.pprint(data['hadoop_data'])
  print
  print '=== Upload/download operations ==='
  pprint.pprint(data['operations'])
  print
  if 'errors' in data:
    print '=== Instance errors ==='
    for msg in data['errors']:
      print msg
    print

  # First the instances, good states first
  if 'instances' in data:
    states = [s[1] for s in util.InstanceState.desc_order
              if s[1] in data['instances']]
    for state in states:
      print '=== {0} ==='.format(state)
      print columnize(sorted(data['instances'][state]))
      print
  print 'Summary: {0}'.format(data['summary'])
  print 'Cluster state: {0}'.format(data['state'])


def wait_for_hadoop():
  """Blocks until the coordinator says Hadoop is ready."""
  print 'Waiting for Hadoop to be ready for jobs...'
  while True:
    state = send_coordinator('/status/cluster', {})
    print '-' * 80
    try:
      pprint_status(state)
      if state['state'] == 'READY':
        break
      if state['state'] == 'BROKEN':
        print 'Oops?'
        sys.exit(1)
    except TypeError:
      print 'The coordinator is not running, or you sent the wrong secret.'
    time.sleep(cfg.poll_delay_secs)
  print


def columnize(raw_entries):
  """Formats of the given list arranged in columns.

  Args:
    raw_entries: A list of strings

  Returns:
    A string of the entries, formatted in the given order from left-to-right and
    then top-to-bottom. Each line is no longer than 80 characters.
  """

  # Pad every entry to the same width
  entry_width = max(map(len, raw_entries))
  entries = [x.ljust(entry_width) for x in raw_entries]
  return '\n'.join(textwrap.wrap('  '.join(entries), width=80,
                                 break_on_hyphens=False))


def script_name():
  """Returns the basename of the current script."""
  return os.path.basename(sys.argv[0])
