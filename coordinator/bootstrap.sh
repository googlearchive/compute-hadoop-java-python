#!/bin/bash
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

set -e
cd ~

echo Starting bootstrap script as ${USER}

# Need java to run the hadoop job controller, and cherrypy has ssl support
sudo apt-get install -y openjdk-6-jre-headless python-cherrypy3 python-openssl

# Set up the REST agent and all of its libraries
sudo easy_install -U bottle
MD=http://metadata/0.1/meta-data/attributes
TARBALL=$(curl ${MD}/tarball)
gsutil cp ${TARBALL} coordinator.tgz
tar xzf coordinator.tgz

# Re-pack tarball for all of the agents we make
cp hadoop/common_snitch.py hadoop/setup_hadoop.py .
mkdir bottle_install
easy_install -zmaxd bottle_install bottle
tar czf snitch-tarball.tgz cfg.py util.py common_snitch.py setup_hadoop.py \
        gcelib bottle_install
rm -rf common_snitch.py setup_hadoop.py bottle_install
BUCKET=$(curl ${MD}/gs_bucket)
gsutil cp snitch-tarball.tgz gs://${BUCKET}/snitch-tarball.tgz

# Log STDOUT and STDERR to a file
exec 3>&1 4>&2 >log_coordinator 2>&1

python coordinator.py &
