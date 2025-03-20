# P4 (4% of grade): SQL and HDFS

## Overview

In this project, you will depoly a data system consisting of an SQL server, an HDFS cluster, a gRPC server, and a client. You will need to read and filter data from a SQL database and persist it to HDFS. Additionally, you will write a fault-tolerant application that works even when an HDFS DataNode fails (we will test this scenario).

Learning objectives:
* communicate with the SQL Server using SQL queries
* use the WebHDFS API
* utilize PyArrow to interact with HDFS and process Parquet files
* handle data loss scenario

Before starting, please review the [general project directions](../projects.md).

## Corrections/Clarifications

- Mar 5: A hint about HDFS environment variables added; a dataflow diagram added; some minor typos fixed.

- Mar 5: Fix the wrong expected file size in Part 1 and sum of blocks in Part 2.

- Mar 6: Released `autobadger` for `p4` (`0.1.6`)

- Mar 7: 
  - Some minor updates on p4 `Readme.md`.
  - Update `autobadgere` to version `0.1.7`
    - Fixed exception handling, now Autobadger can correctly print error messages. 
    - Expanded the expected file size range in test4 `test_Hdfs_size`. 
    - Make the error messages clearer.
  


## Introduction

You'll need to deploy a system including 6 docker containers like this:

<img src="arch.png" width=600>

The data flow roughly follows this:

<img src="dataflow.png" width=600>

We have provided the other components; what you only need is to complete the work within the gRPC server and its Dockerfile.
### Client
This project will use `docker exec` to run the client on the gRPC server's container. Usage of `client.py` is as follows:
```
#Inside the server container
python3 client.py DbToHdfs
python3 client.py BlockLocations -f <file_path>
python3 client.py CalcAvgLoan -c <county_code>
```

### Docker Compose

Take a look at the provided Docker compose file. There are several services, including 3 `datanodes`, a `namenode`, a `SQL server`, a `gRPC Server`. The NameNode service will serve at the host of `boss` within the docker compose network.

### gRPC

You are required to write a server.py and Dockerfile.server based on the provided proto file (you may not modify it).  

The gRPC interfaces have already been defined (see `lender.proto` for details). There are no constraints on the return values of `DbToHdfs`, so you may return what you think is helpful.

### Docker image naming
You need to build the Docker image following this naming convention.
```
docker build . -f Dockerfile.hdfs -t p4-hdfs
docker build . -f Dockerfile.namenode -t p4-nn
docker build . -f Dockerfile.datanode -t p4-dn
docker build . -f Dockerfile.mysql -t p4-mysql
docker build . -f Dockerfile.server -t p4-server
```
### PROJECT environment variable
Note that the compose file assumes there is a "PROJECT" environment
variable.  You can set it to p4 in your environment:

```
export PROJECT=p4
```

**Hint 1:** The command `docker logs <container-name> -f` might be very useful for troubleshooting. It allows you to view real-time output from a specific container.

**Hint 2:** Think about whether there is any .sh script that will help you quickly test code changes.  For example, you may want it to rebuild your Dockerfiles, cleanup an old Compose cluster, and deploy a new cluster.

**Hint 3:** If you're low on disk space, consider running `docker system prune --volumes -f`

## Part 1: `DbToHdfs` gRPC Call

In this part, your task is to implement the `DbToHdfs` gRPC call (you can find the interface definition in the proto file).

**DbToHdfs:** To be more specific, you need to:
1. Connect to the SQL server, with the database name as `CS544` and the password as `abc`. There are two tables in databse: `loans` ,and `loan_types`. The former records all information related to loans, while the latter maps the numbers in the loan_type column of the loans table to their corresponding loan types. There should be **447367** rows in table `loans`. It's like:
   ```mysql
   mysql> show tables;
   +-----------------+
   | Tables_in_CS544 |
   +-----------------+
   | loan_types      |
   | loans           |
   +-----------------+
   mysql> select count(*) from loans;
   +----------+
   | count(*) |
   +----------+
   |   447367 |
   +----------+
   ```
2. What are the actual types for those loans?
   Perform an inner join on these two tables so that a new column `loan_type_name` added to the `loans` table, where its value is the corresponding `loan_type_name` from the `loan_types` table based on the matching `loan_type_id` in `loans`.
3. Filter all rows where `loan_amount` is **greater than 30,000** and **less than 800,000**. After filtering, this table should have only **426716** rows.
4. Upload the generated table to `/hdma-wi-2021.parquet` in the HDFS, with **2x** replication and a **1-MB** block size, using PyArrow (https://arrow.apache.org/docs/python/generated/pyarrow.fs.HadoopFileSystem.html).

To check whether the upload was correct, you can use `docker exec -it <container_name> bash` to enter the gRPC server's container and use HDFS command `hdfs dfs -du -h <path>`to see the file size. The expected result should like:

```
14.4 M   28.9 M  hdfs://nn:9000/hdma-wi-2021.parquet
```
Note: Your file size might have slight difference from this. 
>That's because when we join two tables, rows from one table get matches with rows in the other, but the order of output rows is not guaranteed. If we have the same rows in a different order, the compressibility of snappy (used by Parquet by default) will vary because it is based on compression windows, and there may be more or less redundancy in a window depending on row ordering. 

**Hint 1:** We used similar tables in lecture: https://git.doit.wisc.edu/cdis/cs/courses/cs544/s25/main/-/tree/main/lec/15-sql

**Hint 2:**  To get more familiar with these tables, you can use SQL queries to print the table schema or retrieve sample data. A convenient way to do this is to use `docker exec -it <container name> bash` to enter the SQL Server, then run mysql client `mysql -p CS544` to access the SQL Server and then perform queries.

**Hint 3:** After `docker compose up`, the SQL Server needs some time to load the data before it is ready. Therefore, you need to wait for a while, or preferably, add a retry mechanism for the SQL connection.

**Hint 4:** For PyArrow to be able to connect to HDFS, you'll need to configure some env variables carefully.  Look at how Dockerfile.namenode does this for the start CMD, and do the same in your own Dockerfile for your server.

## Part 2: `BlockLocations` gRPC Call

In this part, your task is to implement the `BlockLocations` gRPC call (you can find the interface definition in the proto file).

**BlockLocations:** To be more specific, for a given file path, you need to return a Python dictionary (that is, a `map` in proto), recording how many blocks are stored by each DataNode (key is the **DataNode location** and value is **number** of blocks on that node).

For example, running `docker exec -it p4-server-1 python3 /client.py BlockLocations -f /hdma-wi-2021.parquet` should show something like this:

```
{'7eb74ce67e75': 15, 'f7747b42d254': 7, '39750756065d': 8}
```

Note: DataNode location is the randomly generated container ID for the
container running the DataNode, so yours will be different, and the
distribution of blocks across different nodes will also likely vary.

The documents [here](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) describe how we can interact with HDFS via web requests. Many [examples](https://requests.readthedocs.io/en/latest/user/quickstart/) show these web requests being made with the curl command, but you'll adapt those examples to use requests.get. By default, WebHDFS runs on port 9870. So use port 9870 instead of 9000 to access HDFS for this part.

Use a `GETFILEBLOCKLOCATIONS` operation to find the block locations.

**Hint:** You have to set appropriate environment variable `CLASSPATH` to access HDFS correctly. See example [here](https://git.doit.wisc.edu/cdis/cs/courses/cs544/s25/main/-/blob/main/lec/18-hdfs/notebook.Dockerfile?ref_type=heads).          

## Part 3: `CalcAvgLoan` gRPC Call

In this part, your task is to implement the `CalcAvgLoan` gRPC call (you can find the interface definition in the proto file).

The call should read hdma-wi-2021.parquet, filtering to rows with the specified county code.  One way to do this would be to pass a `("column", "=", ????)` tuple inside a `filters` list upon read: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html

The call should return the average loan amount from the filtered table as an integer (rounding down if necessary).

As an optimization, your code should also write the filtered data to a file named `partitions/<county_code>.parquet`.  If there are later calls for the same county_code, your code should use the smaller, county-specific Parquet file (instead of filtering the big Parquet file with all loan applications).  The county-specific Parquet file should have 1x replication.  When `CalcAvgLoan` returns the average, it should also use the "source" field to indicate whether the data came from big Parquet file (`source="create"` because a new county-specific file had to be created) or a county-specific file was previously created (`source="reuse"`).

One easy way to check if the county-specific file already exists is to just try reading it with PyArrow.  You should get an `FileNotFoundError` exception if it doesn't exist.

<!--
Imagine a scenario where there could be many queries differentiated by `county`, and one of them is to get the average loan amount for a county. In this case, it might be much more efficient to generate a set of 1x Parquet files filtered by county, and then read data from these partitioned, relatively much smaller tables for computation.

**CalcAvgLoan:** To be more specific, for a given `county_id` , you need to return a int value, indicating the average `loan_amount` of that county. **Note:** You are required to perform this calculation based on the partitioned parquet files generated by `FilterByCounty`. `source` field in proto file can ignored in this part.
-->

After a `DbToHdfs` call and a few `CalcAvgLoan` calls, your HDFS directory structure will look something like this:

      ```
      ├── hdma-wi-2021.parquet
      ├── partitions/
      │   ├── 55001.parquet
      │   ├── 55003.parquet
      │   └── ...
      ```

## Part 4: Fault Tolerance

A "fault" is something that goes wrong, like a hard disk failing or an entire DataNode crashing.  Fault tolerant code continues functioning for some kinds of faults.

In this part, your task is to make `CalcAvgLoan` tolerant to a single DataNode failure (we will kill one during testing!).

Recall that `CalcAvgLoan` sometimes uses small, county-specific Parquet files that have 1x replication, and sometimes it uses the big Parquet file (hdma-wi-2021.parquet) of all loan applications that uses 2x replication.  Your fault tolerance strategy should be as follows:

1. hdma-wi-2021.parquet: if you created this with 2x replication earlier, you don't need to do anything else here, because HDFS can automatically handle a single DataNode failure for you
2. partitions/<COUNTY_CODE>.parquet: this data only has 1x replication, so HDFS might lose it when the DataNode fails.  That's fine, because all the rows are still in the big Parquet file.  You should write code to detect this scenario and recreate the lost/corrupted county-specific file by reading the big file again with the county filter.  If you try to read an HDFS file with missing data using PyArrow, the client will retry for a while (perhaps 30 seconds or so), then raise an OSError exception, which you should catch and handle

CalcAvgLoan should now use the "source" field in the return value to indicate how the average was computed: "create" (from the big file, because a county-specific file didn't already exist), "recreate" (from the big file, because a county-specific file was corrupted/lost), or "reuse" (there was a valid county-specific file that was used).

**Hint:** to manually test DataNode failure, you should use `docker kill` to terminate a node and then wait until you confirm that the number of `live DataNodes` has decreased using the `hdfs dfsadmin -fs <hdfs_path> -report` command. 

## Submission

Read the directions [here](../projects.md) about how to create the
repo.

You have some flexibility about how you write your code, but we must be able to run it like this:

```
docker build . -f Dockerfile.hdfs -t p4-hdfs
docker build . -f Dockerfile.namenode -t p4-nn
docker build . -f Dockerfile.datanode -t p4-dn
docker build . -f Dockerfile.mysql -t p4-mysql
docker build . -f Dockerfile.server -t p4-server
docker compose up -d
```

Then run the client like this:

```
docker exec p4-server-1 python3 /client.py DbToHdfs
docker exec p4-server-1 python3 /client.py BlockLocations -f /hdma-wi-2021.parquet
docker exec p4-server-1 python3 /client.py CalcAvgLoan -c 55001
```

Note that we will copy in the the provided files (docker-compose.yml, client.py, lender.proto, hdma-wi-2021.sql.gz, etc.), overwriting anything you might have changed.  Please do NOT push hdma-wi-2021.sql.gz to your repo because it is large, and we want to keep the repos small.

Please make sure you have `client.py` copied into the p4-server image. We will run client.py in the p4-server-1 container to test your code. 

## Tester

Please be sure that your installed `autobadger` is on version `0.1.7`. You can print the version using

```bash
autobadger --info
```

See [projects.md](https://git.doit.wisc.edu/cdis/cs/courses/cs544/s25/main/-/blob/main/projects.md#testing) for more information.

