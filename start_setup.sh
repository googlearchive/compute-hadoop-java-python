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

# This startup script drops root permissions and executes a setup script passed
# in through the metadata server

set -e
METADATA_URL='http://metadata/0.1/meta-data/attributes'
USERNAME='hadoop'
BOOTSTRAP="/home/${USERNAME}/bootstrap.sh"

adduser ${USERNAME} --disabled-password --shell /bin/bash
echo "${USERNAME} ALL=NOPASSWD: ALL" >> /etc/sudoers
wget ${METADATA_URL}/bootstrap_sh -O ${BOOTSTRAP}
chown ${USERNAME} ${BOOTSTRAP}
chmod +x ${BOOTSTRAP}

# Generate a self-signed cert
sudo apt-get install ssl-cert
# And let our user look at it
sudo usermod -a -G ssl-cert ${USERNAME}
sudo chown ${USERNAME} /etc/ssl/private/ssl-cert-snakeoil.key

sudo -u ${USERNAME} ${BOOTSTRAP}
