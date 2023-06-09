# Benchmarking NewSQL and NoSQL DBMS

The purpose of this repository is to provide a framework for benchmarking various NewSQL and NoSQL database management systems (DBMS) using [YCSB](https://github.com/brianfrankcooper/YCSB). The use case for this project is a social network, which we represented by a schema consisting of three primary aggregates (or tables): users, posts, and comments.


## Repository Structure

The repository is organized into several main directories:

- **schema**: This directory contains the schema for each database. The schema comprises of users, posts, and comments tables, designed to simulate a social network scenario. The corresponding schema for each DBMS (VoltDB, Cassandra, MongoDB, HBase) must be created before running the benchmarks.

- **benchmark**: This folder contains the compiled Java classes for running the benchmarks on each DBMS.

- **YCSB**: Here, you will find the customized connectors extending the DB class of YCSB for each database, and the `MultiTableWorkload.java` class, which extends Workload to manage our custom use case.

- **docker**: This directory contains `docker-compose.yml` files and associated instructions for each DBMS, making the setup and execution process straightforward.

## Running Benchmarks

For setting up the tables and running benchmarks, you can use the following commands:

### Loading Tables
- Cassandra: `./bin/ycsb load cassandra_custom -P workloads/workloada -p workload=site.ycsb.MultiTableWorkload`
- VoltDB: `./bin/ycsb load volt_custom -P workloads/workloada -p workload=site.ycsb.MultiTableWorkload`
- MongoDB: `./bin/ycsb load mongo_custom -P  workloads/workloada -p workload=site.ycsb.MultiTableWorkload`
- HBase: `./bin/ycsb load hbase_custom -P  workloads/workloada -p workload=site.ycsb.MultiTableWorkload`

### Running Benchmarks
- Cassandra: `./bin/ycsb run cassandra_custom -P workloads/workloada -p workload=site.ycsb.MultiTableWorkload`
- VoltDB: `./bin/ycsb run volt_custom -P workloads/workloada -p workload=site.ycsb.MultiTableWorkload`
- MongoDB: `./bin/ycsb run mongo_custom -P  workloads/workloada -p workload=site.ycsb.MultiTableWorkload`
- HBase: `./bin/ycsb run hbase_custom -P  workloads/workloada -p workload=site.ycsb.MultiTableWorkload`

You can further customize your workloads using parameters described in the `example_workload` file.

## Customization and Compilation

To customize the connectors and/or the MultiTableWorkload java class, follow these steps:

1. Clone the YCSB repository.
2. Remove the "checkstyle.xml" from the pom.xml file project since it's not used to format the Java classes.
3. Place the connectors and MultiTableWorkload inside the corresponding project folder. For instance, the CassandraDbCustom connector should be placed inside "ycsb/cassandra/src/main/java/site/" along with the MultiTableWorkload.
4. After compiling with Maven, simply copy the `<name_DBMS>-binding-0.18.0-SNAPSHOT.jar` into the "benchmark/<folder_DBMS>/lib/" directory.

## Performance Comparison Script

The `python` folder contains a script that creates graphical comparisons of CRUD operations for two different benchmarks. To run this script, provide the ".dat" file you generated using time series.
