# AWS EMR Hive Log Analytics

## Project Overview

This project demonstrates how to process large-scale advertising log data using **Amazon EMR, Apache Hadoop, Apache Hive, and Amazon S3**.

The project focuses on analyzing online advertising activity by combining two types of log data:

- **Impression logs:** Records of advertisements displayed to users.
- **Click logs:** Records of users clicking on advertisements.

Using Hive, the two datasets are processed and joined to identify which impressions resulted in clicks. The resulting dataset is stored in Amazon S3 and queried to identify the most-clicked advertisements and the websites generating the most ad clicks.

## Objectives

- Launch and configure an Amazon EMR cluster with Hadoop and Hive.
- Create external Hive tables referencing data stored in Amazon S3.
- Discover and manage table partitions.
- Process log data using HiveQL.
- Join impression and click datasets using a `LEFT OUTER JOIN`.
- Store the processed dataset in Amazon S3.
- Analyze advertising clicks and identify the most effective referring websites.

## Architecture

The workflow uses Amazon S3 for source and output storage, Amazon EMR for distributed processing, and Apache Hive for querying and analysis.

```text
       Advertising Impression Logs
                    |
                    |
                    v
              Amazon S3
                    ^
                    |
       Advertising Click Logs
                    |
                    v
          Amazon EMR Cluster
          (Hadoop and Hive)
                    |
                    v
          Apache Hive Processing
                    |
          +---------+---------+
          |                   |
          v                   v
   Impressions Table     Clicks Table
          |                   |
          +---------+---------+
                    |
                    v
             LEFT OUTER JOIN
                    |
                    v
         Joined Impressions Table
                    |
                    v
             Amazon S3 Output
                    |
                    v
          Hive Analytical Queries
                    |
          +---------+----------+
          |                    |
          v                    v
     Most-Clicked Ads    Top Referrers
```

> **Note:** This is a conceptual representation of the processing workflow. The source log data is provided by the lab environment.

![AWS ETL Architecture](architecture/architecture-diagram.png)
## Technologies Used

| Technology | Purpose |
|---|---|
| Amazon EMR | Provides a managed environment for distributed data processing. |
| Apache Hadoop | Supports distributed processing across cluster nodes. |
| Apache Hive | Enables SQL-like querying and transformation of log data. |
| Amazon S3 | Stores the source logs and processed output data. |
| HiveQL | Defines tables, transforms records, joins datasets, and performs analysis. |
| HDFS | Stores intermediate data used during processing. |
| YARN | Coordinates and schedules distributed processing jobs. |
| AWS Management Console | Used to configure and manage AWS resources. |
| Linux Terminal / SSH | Used to connect to the EMR primary node and execute Hive commands. |

## Dataset

The lab provides advertising log data stored in Amazon S3.

### 1. Impression Logs

The impressions dataset records advertisements displayed to users.

The Hive table references these fields:

| Column | Description |
|---|---|
| `requestBeginTime` | Timestamp associated with the request. |
| `adId` | Identifier of the advertisement. |
| `impressionId` | Identifier used to associate an impression with a click. |
| `referrer` | Website that referred the visitor. |
| `userAgent` | Browser or client information. |
| `userCookie` | User cookie identifier. |
| `ip` | IP address recorded in the log. |
| `dt` | Partition column representing the log date and time. |

### 2. Click Logs

The clicks dataset records advertisement clicks.

| Column | Description |
|---|---|
| `impressionId` | Identifier used to match a click to an impression. |
| `adId` | Identifier of the advertisement. |
| `dt` | Partition column representing the log date and time. |

The `impressionId` field is used to associate click events with their corresponding impressions.

## Implementation

### 1. Launch the Amazon EMR Cluster

Created an EMR cluster configured with Hadoop and Hive using the AWS Management Console.

The lab environment uses an older EMR release and specific instance configurations. These settings are documented as part of the lab exercise rather than as a recommendation for new production deployments.

### 2. Connect to the Cluster

Connected to the EMR primary node through SSH using the lab-provided environment and launched the Hive command-line interface.

Hive commands were used to define tables and process the advertising logs.

### 3. Create External Hive Tables

Created two external tables:

- `impressions`
- `clicks`

Both tables reference existing JSON log data in Amazon S3.

The external table definitions specify the fields required for the analysis without copying the original datasets into HDFS.

### 4. Discover Table Partitions

Used Hive's partition repair command to discover the existing partitions in Amazon S3 and update the Hive metastore.

```sql
SET hive.msck.path.validation=ignore;

MSCK REPAIR TABLE impressions;

MSCK REPAIR TABLE clicks;
```

The lab's impressions dataset contains 241 partitions after partition discovery.

Partitioning allows Hive to organize data by time and helps limit processing to relevant portions of the dataset when queries use partition filters.

### 5. Prepare Intermediate Datasets

Created two temporary Hive tables:

- `tmp_impressions`
- `tmp_clicks`

The intermediate tables use the SequenceFile format and store data in HDFS.

The impressions query converts the request timestamp from milliseconds to a readable timestamp and selects records from the specified time interval.

The clicks query selects the corresponding click records. Its time window extends 20 minutes beyond the impressions interval to account for clicks occurring after an advertisement is displayed.

### 6. Join Impressions and Clicks

Created the `joined_impressions` external table to store the combined dataset in Amazon S3.

A `LEFT OUTER JOIN` matches impressions and clicks using `impressionId`.

```sql
INSERT OVERWRITE TABLE joined_impressions
PARTITION (
    day='${DAY}',
    hour='${HOUR}'
)
SELECT
    i.requestBeginTime,
    i.adId,
    i.impressionId,
    i.referrer,
    i.userAgent,
    i.userCookie,
    i.ip,
    (c.impressionId IS NOT NULL) AS clicked
FROM tmp_impressions i
LEFT OUTER JOIN tmp_clicks c
    ON i.impressionId = c.impressionId;
```

The resulting `clicked` field indicates whether a matching click was found:

- `TRUE`: A matching click exists.
- `FALSE`: No matching click was found.

The left outer join preserves the impression records even when no matching click exists.

The output table is partitioned by `day` and `hour` and stored in Amazon S3 using SequenceFile format.

### 7. Analyze the Joined Dataset

After the join, Hive queries can be used to examine the combined records and identify advertising trends.

#### Preview the processed data

```sql
SELECT *
FROM joined_impressions
LIMIT 10;
```

This query returns a sample of the joined dataset for inspection.

#### Identify the most-clicked advertisements

```sql
SELECT
    adId,
    COUNT(*) AS hits
FROM joined_impressions
WHERE clicked = TRUE
GROUP BY adId
ORDER BY hits DESC
LIMIT 10;
```

This query counts clicked impression records for each advertisement and returns the ten advertisements with the highest counts.

#### Identify the top referring websites

```sql
SELECT
    referrer,
    COUNT(*) AS hits
FROM joined_impressions
WHERE clicked = TRUE
GROUP BY referrer
ORDER BY hits DESC
LIMIT 10;
```

This query identifies the referring websites associated with the highest numbers of clicked impressions.

## Repository Structure

```text
aws-emr-hive-log-analytics/
│
├── README.md
│
├── queries/
│   └── hive_queries.hql
│
├── architecture/
│   └── architecture-diagram.png
│
└── screenshots/
    ├── 01-emr-cluster-configuration.png
    ├── 02-emr-cluster-running.png
    ├── 03-hive-impressions-table.png
    ├── 04-hive-clicks-table.png
    ├── 05-table-partitions.png
    ├── 06-joined-impressions-table.png
    ├── 07-ad-click-analysis.png
    ├── 08-top-referrers-analysis.png
    └── 09-output-data-in-s3.png
```

**Repository contents:**

- `README.md`: Project documentation, architecture, workflow, and analytical queries.
- `queries/hive_queries.hql`: HiveQL statements used throughout the processing workflow.
- `architecture/architecture-diagram.png`: Architecture diagram created for this project.
- `screenshots/`: Screenshots documenting the implementation and results.

Only include files and screenshots that you have actually created or captured. If you have not created the architecture diagram yet, you can remove its reference from the repository structure until it is ready.

## Key Concepts Demonstrated

- Distributed data processing with Amazon EMR and Hadoop.
- SQL-like data processing using Apache Hive.
- External Hive tables referencing Amazon S3 data.
- Partition discovery and metadata management.
- Intermediate data processing using HDFS.
- Timestamp conversion and time-window filtering.
- Joining datasets using a common identifier.
- Writing partitioned output data to Amazon S3.
- Aggregation and ranking using HiveQL.
- Analysis of advertising engagement and referral sources.

## Project Context

This project is based on an AWS Academy lab focused on processing logs with Amazon EMR.

The lab provides the source data and a guided environment for implementing the workflow. The repository documents the technical concepts, queries, architecture, and implementation evidence associated with the exercise.

## Author

**Umair Farooq**

- GitHub: [umair9211](https://github.com/umair9211)
- LinkedIn: [Umair Farooq](https://linkedin.com/in/umair-farooq-6ab9b2201)
