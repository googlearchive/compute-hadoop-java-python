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
exec 3>&1 4>&2 >log_snitch 2>&1

echo Starting bootstrap script as ${USER}

# Always stick hadoop data here, whether or not we have extra disk
sudo mkdir -p /mnt/hadoop

# Set up ephemeral disk, if any
EDISK=/dev/disk/by-id/google-ephemeral-disk-0
if [[ -e ${EDISK} ]]; then
  echo 1 ephemeral disk detected. Setting up...
  sudo /usr/share/google/safe_format_and_mount ${EDISK} /mnt/hadoop
  echo Done setting up ephemeral disk.
fi

# Set up persistent disk, if any
shopt -s nullglob
for dev in /dev/pd_*; do
  name=$(echo ${dev} | sed 's#/dev/pd_##')
  echo Persistent disk ${name} detected. Mounting...
  sudo mkdir -p /mnt/${name}
  sudo mount ${dev} /mnt/${name}
  echo Done setting up persistent disk.
done

sudo chown -R hadoop:hadoop /mnt/hadoop

# Grab our code
MD=http://metadata/0.1/meta-data/attributes
BUCKET=$(curl ${MD}/gs_bucket)
gsutil cp gs://${BUCKET}/snitch-tarball.tgz .
tar xzf snitch-tarball.tgz

# Set up the REST agent
sudo easy_install -H None -f bottle_install -U bottle
wget ${MD}/snitch.py
chmod +x snitch.py

# Setup Hadoop and start snitch
chmod +x setup_hadoop.py
./setup_hadoop.py
