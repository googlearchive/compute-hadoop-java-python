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

"""Generate a secret key to authenticate communication with the coordinator."""



import base64
import os


def generate_password(length):
  return base64.b64encode(os.urandom(length))


def main():
  open('secret', 'w').write('{0}\n'.format(generate_password(128)))
  print 'Password generated.'

if __name__ == '__main__':
  main()
