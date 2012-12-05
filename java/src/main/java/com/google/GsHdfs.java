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

// Direct transfers between GS and HDFS

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

/**
 *
 */
public class GsHdfs {
  FileSystem hdfs;
  Configuration hadoopConf;
  String operation;
  HttpsClient client;

  public GsHdfs(String op) throws Exception {
    operation = op;

    // Initialize HDFS client
    hadoopConf = new Configuration();
    hadoopConf.set("fs.default.name", "hdfs://hadoop-namenode:9000");
    hdfs = FileSystem.get(hadoopConf);

    client = new HttpsClient();
  }

  public void copyGsToHdfs(String gsFn, String hdfsFn) throws Exception {
    Process gsutil = Runtime.getRuntime().exec(new String[] {"gsutil", "cp", gsFn, "-"});
    InputStream src = gsutil.getInputStream();
    FSDataOutputStream dst = hdfs.create(new Path(hdfsFn));
    System.out.println(gsFn + " -> " + hdfsFn);
    doCopy(src, dst, gsFn);
  }

  public void copyHdfsToGs(String hdfsFn, String gsFn) throws Exception {
    Path srcPath = new Path(hdfsFn);
    if (hdfs.isFile(srcPath)) {
      FSDataInputStream src = hdfs.open(srcPath);
      Process gsutil = Runtime.getRuntime().exec(new String[] {"gsutil", "cp", "-", gsFn});
      OutputStream dst = gsutil.getOutputStream();
      System.out.println(hdfsFn + " -> " + gsFn);
      doCopy(src, dst, hdfsFn);
    } else {
      // Recurse
      for (FileStatus file : hdfs.listStatus(srcPath)) {
        Path path = file.getPath();
        copyHdfsToGs(path.toString(), gsFn + "/" + path.getName());
      }
    }
  }

  public void copyWebToHdfs(String urlFn, String hdfsFn) throws Exception {
    URL url = new URL(urlFn);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    connection.connect();
    InputStream src = connection.getInputStream();
    FSDataOutputStream dst = hdfs.create(new Path(hdfsFn));
    System.out.println(urlFn + " -> " + hdfsFn);
    doCopy(src, dst, urlFn);
  }

  private void doCopy(InputStream src, OutputStream dst, String fn) throws IOException {
    // TODO Tune the buffering. GS and HDFS block sizes are much more than 4KB.
    int bufferSize = 4096;
    long reportEvery = 1024 * 1024 * 100;  // every 100MB

    long total = 0;
    long thisRound = 0;
    long startTime = System.currentTimeMillis();
    byte buffer[] = new byte[bufferSize];
    int bytesRead;
    while ((bytesRead = src.read(buffer, 0, bufferSize)) >= 0) {
      dst.write(buffer, 0, bytesRead);
      total += bytesRead;
      thisRound += bytesRead;
      if (thisRound >= reportEvery) {
        long totalMb = total / (1024 * 1024);
        long thisMb = thisRound / (1024 * 1024);
        long now = System.currentTimeMillis();
        long dt = (now - startTime) / 1000;
        int rate = (int) (thisMb / dt);
        String msg = "xfer " + fn + ": " + totalMb + " MB (" + rate + " MB/s)";
        if (operation != null) {
          sendUpdate(msg);
        } else {
          System.out.println(msg);
        }
        thisRound = 0;
        startTime = now;
      }
    }

    // This is particularly necessary when the dst is a pipe to gsutil
    src.close();
    dst.close();
  }

  public void sendUpdate(String msg) throws IOException {
    String data = "state=" + URLEncoder.encode(msg, "UTF-8") + "&operation=" +
        URLEncoder.encode(operation, "UTF-8");
    client.send("https://coordinator:8888/instance/op_status", data);
  }

  public static void main(String[] args) throws Exception {
    String src = args[0];
    String dst = args[1];
    String callback = null;
    if (args.length == 3) {
      callback = args[2];
    }
    GsHdfs xfer = new GsHdfs(callback);

    try {
      if (src.startsWith("gs://")) {
        xfer.copyGsToHdfs(src, dst);
      } else if (dst.startsWith("gs://")) {
        xfer.copyHdfsToGs(src, dst);
      } else {
        xfer.copyWebToHdfs(src, dst);
      }

      if (xfer.operation != null) {
        xfer.sendUpdate("Done");
      }
    } catch (Exception e) {
      xfer.sendUpdate("Error: " + e);
    }
  }
}
