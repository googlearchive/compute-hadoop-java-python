compute-hadoop-java-python
==========================
 
This software demonstrates one way to create and manage a cluster of Hadoop nodes running on Google Compute Engine.

Overview
--------

This software demonstrates one way to create and manage a cluster of Hadoop
nodes running on Google Compute Engine.

Compute can be used to host a Hadoop cluster. One instance is designated as the
"coordinator." In response to authenticated RESTful requests, the coordinator
orchestrates different steps in the lifetime of running a MapReduce job. The
coordinator can launch a cluster, initiate the Hadoop daemons in the correct
order, import data from the web or Google Storage into HDFS, submit MapReduce
jobs, export data from HDFS back to Google Storage, and monitor all of these
steps. The coordinator itself is a thin web server wrapper around a small
Python library that provides a layer of abstraction over the process of
launching and managing instances. The coordinator could also be run from a
regular workstation or in AppEngine.

ONE-TIME SETUP
--------------

1) Run the one-time setup

	$ chmod +x one_time_setup.sh
	$ ./one_time_setup.sh

(If you try to 'source one_time_setup.sh', 'set -e' will remain in effect after
the script completes. This will cause your shell session to end if any command
exits with non-zero status.)

2) Set up firewalls:

	$ gcutil addfirewall snitch --description="Let coordinator and snitches chatter." --allowed="tcp:8888"

8888 is the default port set in cfg.py.

3) Hardcode in your own config

- Set zone, machtype, image, and disk in tools/launch_coordinator.py
- Set the GS bucket and your project ID in tools/common.py. You must set this
  bucket up too:
	`gsutil mb gs://bucket_name`
- Edit hadoop/conf/hadoop-env.sh and tweak things like heap size.

REGULAR OPERATION
-----------------

# Launching a Cluster #

To launch a cluster, run:

	$ ./tools/launch_coordinator.py
	$ ./tools/begin_hadoop.py num_slaves

where num_slaves is the number of slave instances desired.
Your cluster is ready to use once the Hadoop masters are up. You can wait
for all of the slaves by manually polling the cluster status:

	$ ./tools/status.py

See below for an explanation of the states.

The following describes what the above scripts do:

1. The user launches a coordinator instance and provides it the rest of the
   code-base using Google Storage.
2. The user polls an endpoint on the coordinator to know when it's ready, then
   issues a command to launch a Hadoop cluster.
3. The coordinator launches many instances in parallel. Google Storage is used
   to share common configuration and scripts. Two are designated as the Hadoop
   masters: the NameNode and the JobTracker.
4. The instances run their own REST agent, the "snitch," which the coordinator
   polls to know when the instance is finished with setup.
5. When the NameNode is ready, the JobTracker instance's Hadoop daemons can
   begin. When the JobTracker's daemon is running, all of the slaves can connect
   to the masters and join the cluster. When there are at least 3 slaves running
   (this depends on the desired replication value for HDFS), the cluster is
   considered ready for use.
6. During this entire process, the user can poll the coordinator to get
   detailed progress.

# Running MapReduce jobs #

To run a TeraSort benchmark, run the following:

Generate 1TB of data to sort:

	$ ./tools/job_terasort.py 1

Sort the data:

	$ ./tools/job_terasort.py 2

Validate the sort:

	$ ./tools/job_terasort.py 3

Watch progress here through Hadoop's UI

	$ ./tools/ui_links.py

The following describes what the above scripts do:

To import data:

1. The user sends upload requests to the coordinator, specifying a public web
   URL or a Google Storage URI of some input data.
2. The coordinator forwards this request to a Hadoop instance, then returns an
   operation object, which the user can poll for progress.
3. A Hadoop instance pushes the input from the web/GS into HDFS.
To run jobs:

4. The user sends a request to begin a MapReduce job to the coordinator,
   uploading the JAR file using the above process.
5. The user can poll the status of Hadoop jobs, or set up an SSH tunnel to
   access the web UI of the Hadoop JobTracker directly.
To export data:

6. After a job is complete, the user can request that the coordinator exports
   data from HDFS into Google Storage.
7. As before, the coordinator forwards this request to a Hadoop instance, then
   returns a pollable operation.

# Performing other operations #

To add more slaves to an existing cluster:

	$ ./tools/add_slaves.py num_slaves

Although the slaves should become available to existing MapReduce jobs, data in
HDFS will not automatically shifted to them without running a rebalancer.

To export results from HDFS into Google Storage:

	$ ./tools/download_results.py /hdfs/path /gs/path

To destroy your cluster:

	$ ./tools/teardown.py

# STATES #

tools/status.py reports a status for the cluster and for instances.

Instance states:

    BROKEN:       The instance could not finish its startup script. The errors
                  will be propagated to the coordinator and listed in the status
                  of the cluster.
    DOOMED:       The instance is scheduled for deletion.
    NON_EXISTENT: The instance is scheduled to be created.
    PROVISIONING: This is the first Compute state after creating an instance.
    STAGING:      The instance's virtual machine is being set up.
    RUNNING:      The instance is running, but its startup scripts have not yet
                  completed.
    SNITCH_READY: The instance's startup scripts are done.
    HADOOP_READY: The appropriate Hadoop daemons are running.

Cluster states:

    DOWN:        Only the coordinator is running, and no commands have been
                 issued.
    DOOMED:      The coordinator is decomissioning instances.
    BROKEN:      A permanent state indicating that hadoop-jobtracker or
                 hadoop-namenode is BROKEN.
    DOWNLOADING: The coordinator is mirroring the Hadoop package and some other
                 code.
    LAUNCHING:   Some instances exist, but Hadoop is not ready for use yet.
    READY:       Hadoop is usable. Not all slaves may be HADOOP_READY, but enough
                 are.

# DEBUGGING #

You can ssh into any instance. `'sudo su hadoop'` will let you become the user
that all agents run as. `/home/hadoop/log_*` may be useful.

You may have to change hadoop_url in cfg.py to reflect an active Apache mirror.
