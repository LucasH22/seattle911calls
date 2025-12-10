# Seattle Fire Department 911 Dispatches – MPCS 53014 Final Project

This project implements a lambda-architecture application for Seattle Fire Department 911 dispatches. It ingests historical call data into Hive (batch layer), streams new calls from the live Seattle 911 API into Kafka and Spark Streaming (speed layer), and runs a web dashboard via a Node.js/Express app backed by HBase (serve layer).

The dashboard is currently running at http://ec2-52-20-203-80.compute-1.amazonaws.com:3017/.

A 5 minute video demo can be found here: https://www.loom.com/share/7de46827ccd245f4915c74fe316dcf3e

---

## 1. Architecture Overview

**Data sources**

- Historical CSV export of Seattle Fire 911 calls from the City of Seattle open data portal, available at https://data.seattle.gov/Public-Safety/Seattle-Real-Time-Fire-911-Calls/kzjm-xkqj/about_data.
- The batch used in this project covers calls from November 7, 2003, 09:30 AM through November 30, 2025, 8:23 PM.
- Live 911 call data from the Seattle open data API (polled periodically by a Kafka producer), with API documentation at https://dev.socrata.com/foundry/data.seattle.gov/kzjm-xkqj.

**Layers**

- **Batch layer (Hive + HDFS + HBase)**
    - Loads the historical CSV into a Hive external table `lucashou_fire911_csv`.
    - Transforms it into a cleaned ORC table `lucashou_fire911` with parsed timestamps and derived columns:
        - `hour_of_day` (0–23)
        - `day_of_week` (1–7)
    - Aggregates calls by call type into a batch summary table.
    - Materializes those aggregates into an HBase-backed table `lucashou_fire_calls_by_type` (row key = call type).

- **Speed layer (Kafka + Spark Streaming + HBase)**
    - A Java Kafka producer pulls new 911 calls from the Seattle API and publishes JSON records to a Kafka topic:
        - `mpcs53014_lucashou_fire911_v2`
    - A Scala Spark Streaming job consumes from that topic, deduplicates by incident number, and:
        - Updates incremental counts for each call type in `lucashou_fire_calls_by_type_speed`.
        - Maintains the 10 most recent calls in `lucashou_fire911_recent` (rows `pos00`–`pos09`).
        - Uses `lucashou_fire911_seen` to remember which incident numbers have already been processed so replays from Kafka do not double-count calls.

- **Serving layer & web app (HBase REST + Node.js/Express)**
    - A Node.js web app talks to HBase via the REST gateway on the EMR primary node.
    - It combines:
        - Batch counts from `lucashou_fire_calls_by_type`
        - Speed-layer deltas from `lucashou_fire_calls_by_type_speed`
        - Recent incidents from `lucashou_fire911_recent`
    - It displays:
        - A Leaflet map of the most recent calls.
        - A per–call-type stats view (total/day/night).
        - A table of all call types with live totals (batch + streaming).

---

## 2. Repository Layout

```text
lucashou_seattle911calls/
  README.md

  batch_layer/
    lucashou_hive_fire911.hql        # CSV -> lucashou_fire911_csv -> lucashou_fire911 (ORC)
    lucashou_fire_calls_batch.hql    # aggregates by call type into batch summary
    lucashou_fire_calls_hbase.hql    # Hive–HBase mapping + load into lucashou_fire_calls_by_type

  kafka_producer/
    pom.xml
    src/main/java/org/example/StreamFire911IntoKafka.java

  speed_layer/
    pom.xml
    src/main/scala/StreamCalls.scala

  app_seattle911calls/
    package.json
    package-lock.json
    app.js
    alltypes.mustache
    result.mustache
    public/
      index.html
      elegant-aero.css
      table.css
```

## 3. Cluster deployment locations

- **EMR primary node**
  - `/home/hadoop/lucashou/` – contains the two uberjars and Hive scripts:
    - `uber-StreamFire911IntoKafka-1.0-SNAPSHOT.jar`
    - `uber-StreamFire911SpeedLayer-1.0-SNAPSHOT.jar`
    - `lucashou_hive_fire911.hql`, `lucashou_fire_calls_batch.hql`, `lucashou_fire_calls_hbase.hql`
- **HDFS**
  - Historical CSV snapshot directory: `/lucashou/seattle_fire_911`  
    - Can be listed with: `hdfs dfs -ls /lucashou/seattle_fire_911`
- **Web server node**
  - `/home/ec2-user/lucashou/app_seattle911calls` – deployed Node/Express web app that serves the dashboard.

## 4. Cluster Objects

**Hive tables**

- `lucashou_fire911_csv`
    - External CSV table pointing at `/lucashou/seattle_fire_911` in HDFS.
    - Mirrors the schema of the Seattle Real-Time Fire 911 Calls dataset as loaded from the CSV export.
- `lucashou_fire911`
    - ORC table with cleaned records and derived fields (`hour_of_day`, `day_of_week`).
    - Serves as the base table for all batch aggregations.
- `lucashou_fire_calls_by_type_batch` (created inside the batch HQL script)
    - Intermediate batch summary table that aggregates calls by type, and splits them into total/day/night buckets.
    - Used as the source for loading the HBase-backed batch table.
- `lucashou_fire_calls_by_type` (Hive view of HBase)
    - Hive table backed by HBase via `hbase.columns.mapping = ":key,calls:total_calls,calls:day_calls,calls:night_calls"`.
    - Exposes the batch aggregates by call type to both Hive and the web app.

**HBase tables**

- `lucashou_fire_calls_by_type`
    - Column family `calls`.
    - Stores batch-layer counts by call type (total, day, night), one row per call type.
- `lucashou_fire_calls_by_type_speed_v2`
    - Column family `calls`.
    - Stores speed-layer increments (day/night deltas) by call type, updated by Spark Streaming.
- `lucashou_fire911_recent_v2`
    - Column family `loc`.
    - Stores up to 10 most recent calls in rows `pos00`–`pos09` with incident number, type, datetime, latitude, and longitude for the map view.
- `lucashou_fire911_seen`
    - Column family `i`.
    - Tracks which incident numbers have already been processed by the speed layer to avoid double-counting.

**Kafka topic**

- `mpcs53014_lucashou_fire911_v2`
    - Kafka topic used by the Java producer to publish new 911 call records as JSON.
    - The Spark Streaming job consumes from this topic to update the speed-layer HBase tables.

## 5. Prerequisites

- Access to the course EMR cluster with HDFS, Hive, HBase, and Spark configured.
- Access to the course Kafka cluster plus `kafka.client.properties` file.
- Java and Maven installed on local machine and cluster to build/run the producer and Spark job.
- Node.js and npm installed on the machine hosting the web app.
- Socrata API credentials for the Seattle 911 API, set via environment variables on the producer host.
- The HBase REST server enabled and reachable from the web app host.

## 6. End-to-End Workflow

1. Upload the historical Seattle 911 CSV into HDFS.  
2. Run the Hive script to create the external CSV table and the cleaned ORC table with derived time fields.  
3. Run the Hive batch-aggregation script to build the per–call-type summary table.  
4. Create the HBase table for batch summaries and run the Hive HBase script to load the batch aggregates into it.  
5. Build the Kafka producer jar with Maven, transfer uberjar onto cluster, and configure environment variables (API keys).  
6. Start the Kafka producer so it periodically polls the Seattle 911 API and publishes new incidents to the Kafka topic.  
7. Create the three speed-layer HBase tables for seen incidents, per-type speed counts, and the 10 most recent calls.  
8. Build the Spark Streaming speed-layer jar, transfer onto cluster, and submit the streaming job to consume from Kafka and update the HBase speed tables.
9. Install Node.js dependencies for the web app, start the Express app and open the dashboard URL in a browser.


## 7. Limitations and Future Work

This project implements the core pieces of the lambda architecture for Seattle 911 calls (batch, speed, and serving layers). There are several natural extensions that would make it closer to a production system:

- **Raw “data lake” layer for full reprocessing**
    - Today, the historical batch is loaded from a CSV snapshot (2003-11-07 09:30 through 2025-11-30 20:23:00) and then transformed into Hive/HBase tables.
    - A production system would continuously append all raw events from bulk exports into a durable “raw” area in HDFS/S3 and have batch jobs read from that, so any change to the batch logic could be applied by reprocessing the full history from raw.

- **Tuning streaming latency vs. upstream update frequency**
    - The Spark Streaming job uses a small micro-batch interval to keep latency low, even though the upstream Seattle 911 API only updates every few minutes.
    - In a real deployment, the batch interval and producer polling frequency could be tuned together based on SLOs and cluster load.

- **Machine learning / analytics on top of the lambda stack**
    - With the existing batch + speed infrastructure, it would be natural to add models that predict call volume by hour or location, detect anomalies (e.g., spikes in certain incident types), or estimate resource utilization.
    - These models could be trained on historical data in the batch layer and served online with the latest features from the speed layer.

- **Security, access control, and configuration management**
    - Secrets such as API keys are currently provided via environment variables.
    - Future work could integrate with a secrets manager, apply more granular access control to HBase and the web app, and centralize configuration for different environments.