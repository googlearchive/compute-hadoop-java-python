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

// Poll the JobTracker and forward some data to the coordinator

import com.google.gson.Gson;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class HadoopMonitor {
  static final int SLEEP_TIME = 15 * 1000;  // ms
  JobClient jobClient;
  Gson gson;
  HttpsClient client;
  Map<String, JobState> jobs;

  public HadoopMonitor() throws IOException {
    jobClient = new JobClient(new InetSocketAddress("hadoop-jobtracker", 9001),
                           new Configuration());
    gson = new Gson();
    jobs = new HashMap<String, JobState>();
    client = new HttpsClient();
  }

  class JobState {
    public String id, status, failureInfo;
    public float mapProgress, reduceProgress;
    public long elapsedSeconds;
    private transient long startTime, finishTime;

    JobState(JobStatus job) {
      id = job.getJobID().toString();
      status = "PREP";
      startTime = System.currentTimeMillis() / 1000;
      finishTime = 0;
      elapsedSeconds = 0;

      update(job);
    }

    void update(JobStatus job) {
      status = JobStatus.getJobRunState(job.getRunState());

      // Update elapsed time
      if (finishTime == 0) {
        long now = System.currentTimeMillis() / 1000;
        elapsedSeconds = now - startTime;
        if (status.equals("SUCCEEDED") || status.equals("FAILED")) {
          finishTime = now;
        }
      }

      failureInfo = job.getFailureInfo();

      mapProgress = job.mapProgress();
      reduceProgress = job.reduceProgress();
    }
  }

  // Send this back
  class ProgressResult {
    public List<JobState> jobs;
    public int mapreduceNodes, mapTasks, reduceTasks;

    ProgressResult() throws IOException {
      ClusterStatus clusterStatus = jobClient.getClusterStatus();
      mapreduceNodes = clusterStatus.getTaskTrackers();
      mapTasks = clusterStatus.getMapTasks();
      reduceTasks = clusterStatus.getReduceTasks();
    }

    public String toString() {
      return gson.toJson(this);
    }
  }

  public void run () {
    while (true) {
      // Keep trying if there's a problem
      try {
        ProgressResult prog = new ProgressResult();
        for (JobStatus job : jobClient.getAllJobs()) {
          String key = job.getJobID().toString();
          if (jobs.containsKey(key)) {
            jobs.get(key).update(job);
          } else {
            jobs.put(key, new JobState(job));
          }
        }
        prog.jobs = new ArrayList<JobState>(jobs.values());
        String data = "data=" + URLEncoder.encode(prog.toString(), "UTF-8");
        client.send("https://coordinator:8888/hadoop/status_update", data);
      } catch (IOException e) {
        System.err.println("Couldn't get or send progress: " + e);
      }
      try {
        Thread.sleep(SLEEP_TIME);
      } catch (InterruptedException e) {}
    }
  }

  public static void main (String[] args) {
    try {
      HadoopMonitor mon = new HadoopMonitor();
      mon.run();
    } catch (IOException e) {
      // This entire session will now continue without updates from Hadoop. Lose a feature, but
      // not a total show-stopper
      System.err.println("Giving up, couldn't monitor Hadoop: " + e);
    }
  }
}
