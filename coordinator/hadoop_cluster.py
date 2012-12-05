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

"""Hadoop management library."""



import json
import logging
import subprocess
import threading
import time

from cfg import cfg
import gcelib.shortcuts as gce_shortcuts
import util
from util import InstanceState


class CluserState(object):
  """Enum describing the state of the whole cluster."""

  # No other instances are around
  DOWN = (0, 'DOWN')
  # In the process of being destroyed, will enter DOWN when all instances gone
  DOOMED = (1, 'DOOMED')
  # A permanent state meaning a Hadoop master instance didn't make it
  BROKEN = (2, 'BROKEN')
  # Pushing Hadoop and conf to GS
  DOWNLOADING = (3, 'DOWNLOADING HADOOP')
  # Some instances exist, but cluster isn't ready for jobs yet
  LAUNCHING = (4, 'LAUNCHING')
  # Masters and >= cfg.needed_slaves slaves are in HADOOP_READY
  READY = (5, 'READY')


class HadoopCluster(object):
  """Singleton managing creation and monitoring of a cluster of instances."""

  def __init__(self):
    # We're running inside an instance
    cfg.update_from_metadata()
    util.setup_api(service_account=True)

    # Just for addinstance
    self.spawn_scheduler = util.Scheduler(cfg.num_workers)
    # For monitoring, deletion, anything else. We can be doing more of these at
    # a time without threatening API quota limits
    self.other_scheduler = util.Scheduler(cfg.num_workers * 2)
    self.state = CluserState.DOWN
    self.instances = {}
    self.errors = []
    self.first_free_slave = 0
    self.live_slaves = 0
    # For long-running remote tasks, such as transfers. Each operation is a
    # dictionary with state and original parameters.
    self.operations = {}
    self.op_counter = 0
    # This protects writing: self.state, state of each self.instances,
    # self.live_slaves
    self.cv = threading.Condition()
    # This is data forwarded to us about the status of the JobTracker
    self.latest_data = {}
    self.last_update = 0

  # Simple communication with instances

  def update_state(self, instance, state):
    with self.cv:
      if instance == 'cluster':
        old = self.state
        self.state = state
        if old != state:
          logging.info('Cluster now %s', state[1])
      else:
        old = None
        if instance in self.instances:
          old = self.instances[instance]
        self.instances[instance] = state
        if state != old:
          logging.info('%s now %s', instance, state[1])

  # All about launching

  def launch(self, num_slaves):
    """Schedules the launch sequence in the background."""
    if self.state == CluserState.DOWN:
      self.other_scheduler.schedule(self.launch_sequence, (num_slaves,))
      # Don't race and let two launch requests come in
      self.update_state('cluster', CluserState.DOWNLOADING)
      return True
    else:
      return False

  def launch_sequence(self, num_slaves):
    """Mirror the Hadoop binary, then launch instances."""
    # Push Hadoop binary
    subprocess.call(['wget', 'http://{0}/{1}/{1}.tar.gz'.format(cfg.hadoop_url,
                                                                cfg.hadoop_fn)])
    subprocess.call(['gsutil', 'cp', cfg.hadoop_fn + '.tar.gz',
                     cfg.gs_hadoop_tarball])

    # Push Hadoop config
    subprocess.call('tar czf hadoop-conf.tgz hadoop/conf/*', shell=True)
    subprocess.call(['gsutil', 'cp', 'hadoop-conf.tgz', cfg.gs_hadoop_conf])
    subprocess.call(['rm', '-f', 'hadoop-conf.tgz'])

    # Push jar with tools that the NameNode needs
    subprocess.call(['gsutil', 'cp', 'hadoop-tools.jar', cfg.gs_tools_jar])

    # Launch instances
    self.update_state('cluster', CluserState.LAUNCHING)
    # Initialize some state for the masters
    self.update_state(cfg.hadoop_namenode, InstanceState.NON_EXISTENT)
    self.update_state(cfg.hadoop_jobtracker, InstanceState.NON_EXISTENT)
    self.spawn_scheduler.schedule(self.launch_nn, ())
    self.spawn_scheduler.schedule(self.launch_jt, ())
    self.add_slaves(num_slaves)

  def spawn_instance(self, name, snitch):
    """Create an instance with the specified snitch."""
    disks = []
    if cfg.disk:
      # Can't mount rw if others have already mounted it ro... so just only
      # mount it on one instance
      if cfg.rw_disk_instance:
        if cfg.rw_disk_instance == name:
          disks = gce_shortcuts.rw_disks([cfg.disk])
        # Otherwise, don't mount
      else:
        # Everyone gets it ro
        disks = gce_shortcuts.ro_disks([cfg.disk])
    network = []
    # Always give the JobTracker and NameNode an external IP.
    if (not name.startswith('hadoop-slave-')) or cfg.external_ips:
      network = gce_shortcuts.network()
    else:
      network = gce_shortcuts.network(use_access_config=False)
    if name == cfg.hadoop_namenode:
      # This instance handles transfers from HDFS to GS.
      scope = cfg.rw_storage_scope
    else:
      scope = cfg.ro_storage_scope
    resp = util.api.insert_instance(
        name=name, zone=cfg.zone,
        machineType=cfg.machine_type, image=cfg.image,
        serviceAccounts=gce_shortcuts.service_accounts([scope]),
        disks=disks,
        metadata=gce_shortcuts.metadata({
            'gs_bucket': cfg.gs_bucket,
            'snitch-tarball.tgz': cfg.gs_snitch_tarball,
            'startup-script': open('start_setup.sh').read(),
            'bootstrap.sh': open('hadoop/bootstrap.sh').read(),
            'snitch.py': open(snitch).read()
        }),
        networkInterfaces=network,
        blocking=True
    )
    return not 'error' in resp

  def new_slave_names(self, num):
    # Callers should assume these slaves will be created
    start = self.first_free_slave
    self.first_free_slave = start + num
    return ['hadoop-slave-{0:03d}'.format(x) for x in range(start, start + num)]

  def masters_up(self):
    result = False
    with self.cv:
      nn = self.instances[cfg.hadoop_namenode] == InstanceState.HADOOP_READY
      jt = self.instances[cfg.hadoop_jobtracker] == InstanceState.HADOOP_READY
      result = nn and jt
    return result

  def start_slave(self, name):
    assert self.masters_up()
    util.checked_do(name, '/start', {})
    with self.cv:
      self.update_state(name, InstanceState.HADOOP_READY)
      self.live_slaves += 1
      if self.live_slaves >= cfg.needed_slaves:
        self.update_state('cluster', CluserState.READY)

  def launch_nn(self):
    """Create and monitor the instance running the NameNode."""
    # Keep this on the spawn_scheduler because it's hi-pri
    if not self.spawn_instance(cfg.hadoop_namenode,
                               'hadoop/namenode_snitch.py'):
      self.update_state('cluster', CluserState.BROKEN)
      return

    if not self.monitor_instance(cfg.hadoop_namenode,
                                 InstanceState.SNITCH_READY):
      self.update_state('cluster', CluserState.BROKEN)
      return

    with self.cv:
      # As part of the namenode's startup, it actually gets Hadoop running
      self.update_state(cfg.hadoop_namenode, InstanceState.HADOOP_READY)
      self.cv.notifyAll()

  def launch_jt(self):
    """Create and monitor the instance running the Jobtracker.

    This also blocks and waits for the NameNode, then starts the JobTracker and
    starts the agent that monitors Hadoop.
    """
    # Keep this on the spawn_scheduler because it's hi-pri
    if not self.spawn_instance(cfg.hadoop_jobtracker,
                               'hadoop/jobtracker_snitch.py'):
      self.update_state('cluster', CluserState.BROKEN)
      return

    if not self.monitor_instance(cfg.hadoop_jobtracker,
                                 InstanceState.SNITCH_READY):
      self.update_state('cluster', CluserState.BROKEN)
      return
    with self.cv:
      while self.instances[cfg.hadoop_namenode] != InstanceState.HADOOP_READY:
        self.cv.wait()
    util.checked_do(cfg.hadoop_jobtracker, '/start', {})
    self.update_state(cfg.hadoop_jobtracker, InstanceState.HADOOP_READY)
    # Fork off and start our Java Hadoop monitor
    util.bg_exec(
        ['java', '-cp', 'hadoop-tools.jar', 'com.google.HadoopMonitor'],
        '/home/hadoop/monitor_log'
    )
    with self.cv:
      self.cv.notifyAll()

  def launch_slave1(self, name):
    """Create the slave, then move to a different queue to finish."""
    if self.spawn_instance(name, 'hadoop/slave_snitch.py'):
      # Assume they're at least in this state. If we shove them on the
      # other_scheduler's queue and we don't get to them for a while, it appears
      # as if we haven't even started the instance yet
      self.update_state(name, InstanceState.PROVISIONING)
      self.other_scheduler.schedule(self.launch_slave2, (name,))

  def launch_slave2(self, name):
    """Check to see if the slave's Hadoop daemons can be started yet."""
    # Don't continuously monitor it; just poke it and yield if it's not ready.
    # That way we cycle through all pending slaves quickly and promote the ones
    # from SNITCH_READY to HADOOP_READY as fast as possible.
    # The flow, though, is monitor -> wait for masters -> start slave

    # Monitor
    if self.instances[name] != InstanceState.SNITCH_READY:
      status, err = util.get_status(name)
      self.update_state(name, status)
      if status == InstanceState.BROKEN:
        self.instance_fail(name, err)
        return

    # Are masters up?
    if self.instances[name] == InstanceState.SNITCH_READY:
      if self.masters_up():
        self.start_slave(name)
        # Done!
        return

    # If we fall-through, schedule it for later
    self.other_scheduler.schedule(self.launch_slave2, (name,))

  # Returns True on success, False if BROKEN
  def monitor_instance(self, name, wait_for_state=InstanceState.RUNNING):
    """Blockingly poll an instance until it reaches the requested state."""
    # get_status() doesn't know about this state
    assert wait_for_state is not InstanceState.HADOOP_READY
    while True:
      status, err = util.get_status(name)
      self.update_state(name, status)
      if status == InstanceState.BROKEN:
        self.instance_fail(name, err)
        return False
      if status >= wait_for_state:
        break
      time.sleep(cfg.poll_delay_secs)
    return True

  # Other interactions with the cluster

  def new_op(self):
    name = 'xfer_{0}'.format(self.op_counter)
    self.op_counter += 1
    self.operations[name] = {'operation': name, 'state': 'Requested'}
    return name

  def op_status(self, name, msg):
    self.operations[name]['state'] = msg
    logging.info('%s: %s', name, msg)

  def transfer(self, src, dst):
    # returns None if there's a problem, otherwise the operation name to poll
    if self.state != CluserState.READY:
      return None
    op = self.new_op()
    self.operations[op]['src'] = src
    self.operations[op]['dst'] = dst
    util.checked_do(cfg.hadoop_namenode, '/transfer', {'src': src, 'dst': dst,
                                                       'operation': op})
    return self.operations[op]

  def submit_job(self, jar, job_args):
    if self.state == CluserState.READY:
      util.checked_do(cfg.hadoop_jobtracker, '/job/start',
                      {'jar': jar, 'args': json.dumps(job_args)})
      return True
    else:
      return False

  def clean_hdfs(self, path):
    """Recursively deletes files from a HDFS path."""
    util.checked_do(cfg.hadoop_namenode, '/clean', {'path': path})
    return True

  def add_slaves(self, num_slaves):
    if self.state >= CluserState.LAUNCHING:
      slaves = self.new_slave_names(num_slaves)
      # Initialize some state for them
      for name in slaves:
        self.update_state(name, InstanceState.NON_EXISTENT)
        self.spawn_scheduler.schedule(self.launch_slave1, (name,))
      return True
    else:
      return False

  def nix(self, name):
    util.api.delete_instance(name, blocking=True)
    with self.cv:
      del self.instances[name]
      if not self.instances:
        assert self.state is CluserState.DOOMED
        self.update_state('cluster', CluserState.DOWN)

  def status(self):
    # Don't need to lock; we're just reading

    # Group instances by state
    states = util.MultiDict()
    for instance, status in self.instances.items():
      states.add(status[1], instance)

    return {'instances': states.jsonify(),
            'summary': str(states),
            'state': self.state[1],
            'errors': self.errors}

  # An instance had some problem that they want us to log.
  # They're not necessarily broken
  def instance_fail(self, name, reason):
    msg = '{0}: {1}'.format(name, reason)
    logging.warn(msg)
    self.errors.append(msg)
