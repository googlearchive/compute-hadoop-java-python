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

"""A small REST server running on the coordinator instance."""



import copy
import json
import logging
import time

import bottle
from cfg import cfg
import cherrypy.wsgiserver
import hadoop_cluster


def reply(data):
  return json.dumps(data) + '\n'


def reply_ok(result):
  if result:
    return reply({'result': 'ok'})
  else:
    return reply({'result': 'failed'})


def authorize():
  their_secret = None
  if 'secret' in bottle.request.forms:
    their_secret = bottle.request.forms.get('secret')
  if their_secret != cfg.secret:
    logging.info('%s requested %s with data %s',
                 bottle.request.headers.get('Host'), bottle.request.fullpath,
                 bottle.request.forms.items())
    bottle.abort(401, 'Your request does not include the right authorization.')


def authorize_internal():
  """Check the request originates from another instance."""
  sender = bottle.request['REMOTE_ADDR']
  if not sender.startswith('10.'):
    logging.info('Untrusted %s requested %s with data %s',
                 sender, bottle.request.fullpath, bottle.request.forms.items())
    bottle.abort(401, 'Your request does not include the right authorization.')


def main():
  cluster = hadoop_cluster.HadoopCluster()

  app = bottle.Bottle()

  # This is just used to detect when the coordinator is set up
  @app.route('/status')
  def status():
    return reply({'state': 'READY'})

  @app.post('/hadoop/launch')
  def launch_hadoop():
    authorize()
    num_slaves = int(bottle.request.forms.get('num_slaves'))
    logging.info('launch with %s requested', num_slaves)
    return reply_ok(cluster.launch(num_slaves))

  @app.post('/hadoop/add_slaves')
  def add_hadoop_slaves():
    authorize()
    num_slaves = int(bottle.request.forms.get('num_slaves'))
    logging.info('add_slaves with %s requested', num_slaves)
    return reply_ok(cluster.add_slaves(num_slaves))

  @app.post('/transfer')
  def transfer():
    authorize()
    src = bottle.request.forms.get('src')
    dst = bottle.request.forms.get('dst')
    logging.info('transfer %s -> %s requested', src, dst)
    op = copy.copy(cluster.transfer(src, dst))
    if op:
      op['result'] = 'ok'
      return reply(op)
    else:
      return reply({'result': 'failed'})

  @app.post('/job/clean')
  def clean_job():
    authorize()
    path = bottle.request.forms.get('path')
    logging.info('clean hdfs %s requested', path)
    return reply_ok(cluster.clean_hdfs(path))

  @app.post('/job/submit')
  def submit_job():
    authorize()
    jar = bottle.request.forms.get('jar')
    job_args = map(str, json.loads(bottle.request.forms.get('job_args')))
    logging.info('job submission requested: %s %s', jar, job_args)
    return reply_ok(cluster.submit_job(jar, job_args))

  @app.post('/status/cluster')
  def cluster_status():
    authorize()
    response = cluster.status()
    response['hadoop_staleness'] = int(time.time() - cluster.last_update)
    response['hadoop_data'] = cluster.latest_data
    response['operations'] = cluster.operations
    return reply(response)

  @app.post('/status/op/<name>')
  def get_op_status(name):
    authorize()
    return reply(cluster.operations[name])

  # Internal calls below

  # This is for the java piece to tell us about Hadoop
  @app.post('/hadoop/status_update')
  def hadoop_status_update():
    authorize_internal()
    cluster.latest_data = json.loads(bottle.request.forms.get('data'))
    cluster.last_update = time.time()
    return '\n'

  @app.post('/instance/report_fail')
  def report_instance_fail():
    authorize_internal()
    name = bottle.request.forms.get('name')
    msg = bottle.request.forms.get('msg')
    cluster.instance_fail(name, msg)
    return '\n'

  @app.post('/instance/op_status')
  def report_op_status():
    authorize_internal()
    op = bottle.request.forms.get('operation')
    state = bottle.request.forms.get('state')
    cluster.op_status(op, state)
    return '\n'

  print 'Starting coordinator server...'
  # Bottle's wrapper around cherrypy doesn't let us setup SSL, so do this
  # ourselves
  server = cherrypy.wsgiserver.CherryPyWSGIServer(('0.0.0.0', cfg.port), app)
  server.quiet = True
  server.ssl_certificate = '/etc/ssl/certs/ssl-cert-snakeoil.pem'
  server.ssl_private_key = '/etc/ssl/private/ssl-cert-snakeoil.key'
  try:
    logging.info('Coordinator agent launched')
    server.start()
  finally:
    server.stop()

if __name__ == '__main__':
  main()
