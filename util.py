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

"""Utility functions for local scripts and all instances."""



import collections
import json
import logging
import multiprocessing
import Queue
import socket
import subprocess
import threading
import time
import urllib

import httplib2

from cfg import cfg
import gcelib.gce_util
import gcelib.gce_v1beta12

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(filename)s:%(lineno)s [%(levelname)s]  %(message)s',
    datefmt='%x %X'
)


# Access to the Compute API

api = None


def setup_api(service_account=True):
  """Sets up a usable Compute API object.

  Args:
    service_account: If true, authorize using service accounts. This only works
      on the coordinator instance. Otherwise, authorize using a local config
      file or using the webserver oauth flow.
  """

  global api
  if service_account:
    creds = gcelib.gce_util.ServiceAccountCredentials()
  else:
    creds = gcelib.gce_util.get_credentials()
  api = gcelib.gce_v1beta12.GoogleComputeEngine(
      creds, default_project=cfg.project_id,
      logging_level=logging.ERROR)


def get_instance_names():
  return [instance.name for instance in api.all_instances()
          if (instance.name == cfg.coordinator or
              instance.name == cfg.hadoop_jobtracker or
              instance.name == cfg.hadoop_namenode or
              instance.name.startswith('hadoop-slave-'))]


class InstanceState(object):
  """Enum describing the state of an instance."""
  # Snitch reported a problem
  BROKEN = (0, 'BROKEN')
  # About to be deleted
  DOOMED = (1, 'DOOMED')
  # getinstance doesn't know of it yet
  NON_EXISTENT = (2, 'NON_EXISTENT')
  # These are normal Compute states
  PROVISIONING = (3, 'PROVISIONING')
  STAGING = (4, 'STAGING')
  RUNNING = (5, 'RUNNING')
  # The snitch has been started
  SNITCH_READY = (6, 'SNITCH_READY')
  # Whatever hadoop daemon particular to this instance is running
  HADOOP_READY = (7, 'HADOOP_READY')
  desc_order = [HADOOP_READY, SNITCH_READY, RUNNING, STAGING, PROVISIONING,
                NON_EXISTENT, DOOMED, BROKEN]


def get_status(name):
  """Get the status of an instance.

  Args:
    name: Instance name

  Returns:
    A tuple (state, extra_info). extra_info is a string only used when the
    instance is broken somehow. This won't ever report HADOOP_READY; that's
    known when we start a Hadoop daemon ourselves.
  """
  # Do getinstance first. Trying to poke the agent on a STAGING box just times
  # out, so such stages are never observed otherwise
  try:
    data = api.get_instance(name)
    if data.status == 'RUNNING':
      # Now try talking to their agent
      address = name_to_ip(name, data=data) if cfg.ip_via_api else name
      response = talk_to_agent(address, '/status')
      if response is not None:
        state = response.get('state', '')
        if state == 'READY':
          return (InstanceState.SNITCH_READY, None)
        elif state != 'STARTING':
          msg = 'snitch reported {0}'.format(response['state'])
          logging.warn('%s: %s', name, msg)
          return (InstanceState.BROKEN, msg)
      return (InstanceState.RUNNING, None)
    elif data.status == 'PROVISIONING':
      return (InstanceState.PROVISIONING, None)
    elif data.status == 'STAGING':
      return (InstanceState.STAGING, None)
    else:
      msg = 'instance is {0}'.format(data.status)
      logging.warn('%s: %s', name, data.status)
      return (InstanceState.BROKEN, msg)
  except ValueError:
    return (InstanceState.NON_EXISTENT, None)


# Communication

ip_cache = {}


def name_to_ip(name, data=None):
  """Do a DNS lookup using the Compute API.

  Args:
    name: instance name
    data: the result from calling getinstance, if the caller already has it.

  Returns:
    An IP address, unless some error is raised.
  """
  if name in ip_cache:
    return ip_cache[name]
  else:
    if data is None:
      try:
        data = api.get_instance(name)
      except ValueError:
        # This instance does not exist
        return None
    ip = data.networkInterfaces[0].accessConfigs[0].natIP
    ip_cache[name] = ip
    return ip


def talk_to_agent(address, method, data=None):
  """Make a REST call. These are described in docs/API.

  Args:
    address: IP address from name_to_ip() or a hostname (if called from an
             instance)
    method: the HTTP call to make, should include the leading /
    data: a Python dictionary; caller must JSONify things themselves.

  Returns:
    The reply, which will be a de-JSONified dictionary.
  """
  try:
    url = 'https://{0}:{1}{2}'.format(address, cfg.port, method)
    # The coordinator's certificate is self-signed, so we cannot verify we are
    # talking to the "correct" coordinator. Eavesdropping is not a problem, but
    # man-in-the-middle attacks could be.
    http = httplib2.Http(disable_ssl_certificate_validation=True, timeout=5)
    if data is None:
      # GET
      return json.loads(http.request(url, 'GET')[1])
    else:
      # POST
      return json.loads(http.request(url, 'POST', urllib.urlencode(data))[1])
  except (httplib2.HttpLib2Error, socket.error, ValueError):
    return None


def checked_do(who, command, data=None):
  """Issue a rest call and verify the response indicates no errors."""
  address = name_to_ip(who) if cfg.ip_via_api else who
  result = talk_to_agent(address, command, data=data)
  if result is None or result['result'] != 'ok':
    raise Exception('{0}{1} failed: {2}'.format(who, command, result))
  return result


# Process management


def bg_exec(args, log=None):
  """Run a command in a different process, optionally logging.

  Args:
    args: argv in list form
    log: filename where STDOUT and STDERR should be logged, or None
  """

  def run(proc_args, log):
    if log is not None:
      out = open(log, 'w')
      subprocess.call(proc_args, stdout=out, stderr=out)
    else:
      subprocess.call(proc_args)
  multiprocessing.Process(target=run, args=(args, log)).start()


def retry_call(run, fail_cb=None):
  """Try to execute a command cfg.download_attempts times.

  Args:
    run: list of strings for the command and args to run
    fail_cb: called with a string message describing the problem.

  Raises:
    CalledProcessError: if all attempts fail
  """
  last_retcode = None
  cmd = ' '.join(run)
  for i in range(0, cfg.download_attempts):
    logging.info('Attempt %s at running %s', i + 1, cmd)
    # Let STDOUT trickle down to the log file
    proc = subprocess.Popen(run, stderr=subprocess.PIPE)
    _, stderr = proc.communicate()
    last_retcode = proc.returncode
    if last_retcode:
      msg = 'Attempt {0} at {1} failed ({2}): {3}'.format(i + 1, repr(cmd),
                                                          last_retcode,
                                                          stderr)
      logging.warn(msg)
      if fail_cb is not None:
        fail_cb(msg)
      time.sleep(cfg.poll_delay_secs)
    else:
      # Done!
      return
  # All attempts failed
  raise subprocess.CalledProcessError(last_retcode, run[0])

# Data structure


class MultiDict(object):
  """Maintain a map from keys to a set of values."""

  def __init__(self):
    self.multidict = collections.defaultdict(set)

  def add(self, key, value):
    self.multidict[key].add(value)

  def remove(self, key, value):
    self.multidict[key].remove(value)
    if not self.multidict[key]:
      del self.multidict[key]

  def __str__(self):
    """Describe how many values of each key the multidict holds."""
    return ', '.join(['{0} {1}'.format(len(self.multidict[key]), key)
                      for key in self.multidict])

  def jsonify(self):
    simple = {}
    for key, values in self.multidict.items():
      simple[key] = list(values)
    return simple

# Parallelization


class Task(object):
  def __init__(self, run, args):
    self.run = run
    assert type(args) is tuple
    self.args = list(args)


class Worker(threading.Thread):
  def __init__(self, scheduler):
    threading.Thread.__init__(self)
    self.scheduler = scheduler
    self.daemon = True  # Exit when only workers are left

  def run(self):
    while True:
      task = self.scheduler.queue.get()
      task.run(*(task.args))


class Scheduler(object):
  def __init__(self, num_workers):
    self.queue = Queue.Queue()

    # Launch a thread pool
    for _ in range(0, num_workers):
      Worker(self).start()

  def schedule(self, run, args):
    # Args is a tuple
    task = Task(run, args)
    self.queue.put(task)
