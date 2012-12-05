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

"""Common code among snitches."""



import json
import logging
import sys

import bottle
from cfg import cfg
import cherrypy.wsgiserver


def authorize():
  """Check the request originates from another instance."""
  sender = bottle.request['REMOTE_ADDR']
  if not sender.startswith('10.'):
    logging.info('Untrusted %s requested %s with data %s',
                 sender, bottle.request.fullpath, bottle.request.forms.items())
    bottle.abort(401, 'Your request does not include the right authorization.')


def start_snitch(app):
  """Set up a status handler and launch the snitch's webserver."""
  cfg.update_from_metadata()
  state = sys.argv[1]

  # The coordinator will poll this
  @app.route('/status')
  def status():
    return json.dumps({'state': state}) + '\n'

  # Bottle's wrapper around cherrypy doesn't let us setup SSL, so do this
  # ourselves
  server = cherrypy.wsgiserver.CherryPyWSGIServer(('0.0.0.0', cfg.port), app)
  server.quiet = True
  server.ssl_certificate = '/etc/ssl/certs/ssl-cert-snakeoil.pem'
  server.ssl_private_key = '/etc/ssl/private/ssl-cert-snakeoil.key'
  try:
    server.start()
  finally:
    server.stop()
