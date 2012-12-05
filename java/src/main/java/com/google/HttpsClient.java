package com.google;

/* Copyright 2012 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Utility class to POST to HTTPS endpoints with self-signed certificates

import java.io.IOException;

/**
 *
 */
class HttpsClient {
  public HttpsClient() {}

  public void send(String address, String data) throws IOException {
    // HTTPS with self-signed certificates in Java takes too much effort to get right
    String command[] = {"curl", address, "-k", "-d", data};
    Runtime.getRuntime().exec(command);
  }
}
