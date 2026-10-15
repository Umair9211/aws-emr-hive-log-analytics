-- ============================================================
-- AWS EMR HIVE LOG ANALYTICS
-- Project: Processing Logs by Using Amazon EMR
-- Purpose: Analyze advertising impressions and clicks
-- ============================================================


-- ============================================================
-- 1. CREATE THE IMPRESSIONS EXTERNAL TABLE
-- ============================================================

CREATE EXTERNAL TABLE impressions (
    requestBeginTime STRING,
    adId STRING,
    impressionId STRING,
    referrer STRING,
    userAgent STRING,
    userCookie STRING,
    ip STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
    'paths' = 'requestBeginTime, adId, impressionId, referrer, userAgent, userCookie, ip'
)
LOCATION '${SAMPLE}/tables/impressions';


-- ============================================================
-- 2. DISCOVER IMPRESSIONS TABLE PARTITIONS
-- ============================================================

SET hive.msck.path.validation=ignore;

MSCK REPAIR TABLE impressions;

-- Inspect the table schema and metadata
DESCRIBE FORMATTED impressions;

-- View discovered partitions
SHOW PARTITIONS impressions;


-- ============================================================
-- 3. CREATE THE CLICKS EXTERNAL TABLE
-- ============================================================

CREATE EXTERNAL TABLE clicks (
    impressionId STRING,
    adId STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
    'paths' = 'impressionId'
)
LOCATION '${SAMPLE}/tables/clicks';


-- ============================================================
-- 4. DISCOVER CLICKS TABLE PARTITIONS
-- ============================================================

MSCK REPAIR TABLE clicks;

-- Inspect the table schema
DESCRIBE FORMATTED clicks;

-- View discovered partitions
SHOW PARTITIONS clicks;


-- ============================================================
-- 5. CREATE THE JOINED IMPRESSIONS EXTERNAL TABLE
-- ============================================================

CREATE EXTERNAL TABLE joined_impressions (
    requestBeginTime STRING,
    adId STRING,
    impressionId STRING,
    referrer STRING,
    userAgent STRING,
    userCookie STRING,
    ip STRING,
    clicked BOOLEAN
)
PARTITIONED BY (
    day STRING,
    hour STRING
)
STORED AS SEQUENCEFILE
LOCATION '${OUTPUT}/tables/joined_impressions';


-- ============================================================
-- 6. CREATE A TEMPORARY IMPRESSIONS TABLE
-- ============================================================

CREATE TABLE tmp_impressions (
    requestBeginTime STRING,
    adId STRING,
    impressionId STRING,
    referrer STRING,
    userAgent STRING,
    userCookie STRING,
    ip STRING
)
STORED AS SEQUENCEFILE;


-- ============================================================
-- 7. LOAD IMPRESSIONS FOR THE SELECTED TIME PERIOD
-- ============================================================

INSERT OVERWRITE TABLE tmp_impressions
SELECT
    FROM_UNIXTIME(
        CAST(
            (CAST(i.requestBeginTime AS BIGINT) / 1000)
            AS INT
        )
    ) AS requestBeginTime,
    i.adId,
    i.impressionId,
    i.referrer,
    i.userAgent,
    i.userCookie,
    i.ip
FROM impressions i
WHERE i.dt >= '${DAY}-${HOUR}-00'
  AND i.dt < '${NEXT_DAY}-${NEXT_HOUR}-00';


-- ============================================================
-- 8. CREATE A TEMPORARY CLICKS TABLE
-- ============================================================

CREATE TABLE tmp_clicks (
    impressionId STRING,
    adId STRING
)
STORED AS SEQUENCEFILE;


-- ============================================================
-- 9. LOAD CLICKS FOR THE SELECTED TIME PERIOD
-- Includes an additional 20-minute window for later clicks
-- ============================================================

INSERT OVERWRITE TABLE tmp_clicks
SELECT
    impressionId,
    adId
FROM clicks c
WHERE c.dt >= '${DAY}-${HOUR}-00'
  AND c.dt < '${NEXT_DAY}-${NEXT_HOUR}-20';


-- ============================================================
-- 10. JOIN IMPRESSIONS WITH CLICKS
-- LEFT OUTER JOIN preserves all impressions.
-- clicked = TRUE when a matching click is found.
-- ============================================================

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


-- ============================================================
-- 11. PREVIEW THE JOINED DATASET
-- ============================================================

SET hive.cli.print.header=true;

SELECT *
FROM joined_impressions
LIMIT 10;


-- ============================================================
-- 12. FIND THE MOST-CLICKED ADVERTISEMENTS
-- ============================================================

SELECT
    adId,
    COUNT(*) AS hits
FROM joined_impressions
WHERE clicked = TRUE
GROUP BY adId
ORDER BY hits DESC
LIMIT 10;


-- ============================================================
-- 13. FIND THE TOP REFERRING WEBSITES
-- ============================================================

SELECT
    referrer,
    COUNT(*) AS hits
FROM joined_impressions
WHERE clicked = TRUE
GROUP BY referrer
ORDER BY hits DESC
LIMIT 10;
