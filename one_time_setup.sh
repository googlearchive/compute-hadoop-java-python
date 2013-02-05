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

# This script performs one-time setup required before the main tools will work.
# If you need to get back to a clean state after running it, run:
# rm -rf gcelib* tools/cfg.py tools/util.py tools/gcelib secret java/target hadoop-tools.jar *.pyc tools/*.pyc

# Latest version of gcelib.
GCELIB_VERS=0.3.0

set -e

if [ ! -f one_time_setup.sh ]; then
  echo "Run this script from the directory containing one_time_setup.sh"
  exit 1
fi

chmod +x tools/*.py
chmod -x tools/common.py

echo "Setting up gcelib..."
wget --quiet http://google-compute-engine-tools.googlecode.com/files/gcelib-$GCELIB_VERS.tar.gz
tar xzf gcelib-$GCELIB_VERS.tar.gz
rm -f gcelib-$GCELIB_VERS.tar.gz
mv gcelib-$GCELIB_VERS/gcelib real_gcelib
rm -rf gcelib-$GCELIB_VERS
mv real_gcelib gcelib
ln -s ../gcelib tools/gcelib
./tools/authorize_gce.py
echo

echo "Linking modules..."
ln -s ../util.py tools/util.py
ln -s ../cfg.py tools/cfg.py
echo

echo "Building java component..."
command -v mvn > /dev/null 2>&1 || {
  echo >&2 "You will need to install maven."
  echo >&2 "Run 'sudo apt-get install maven' if you are using a \
Debian-based distribution, or see http://maven.apache.org/ otherwise."
  exit 1
}
cd java
mvn -q assembly:assembly
cp target/hadoop-tools-1.0-SNAPSHOT-jar-with-dependencies.jar ../hadoop-tools.jar
cd ..
echo

echo "Generating a secret to demonstrate your identity to the coordinator..."
./tools/gen_passwd.py
echo

echo "Done! Continue following along in README."
