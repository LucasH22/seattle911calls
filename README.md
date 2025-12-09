# Seattle 911 Calls – Lambda Architecture Project

This project implements a lambda-architecture application for Seattle Fire Department 911 dispatches. It ingests historical call data into Hive (batch layer), streams new calls from the live Seattle 911 API into Kafka and Spark Streaming (speed layer), and runs a web dashboard via a Node.js/Express app backed by HBase (serve layer).

The dashboard is currently running at http://ec2-52-20-203-80.compute-1.amazonaws.com:3017/.

---

## 1. Architecture Overview

**Data sources**

- Historical CSV export of Seattle Fire 911 calls from the City of Seattle open data portal, available at https://data.seattle.gov/Public-Safety/Seattle-Real-Time-Fire-911-Calls/kzjm-xkqj/about_data.
- The batch used in this project covers calls from November 7, 2003, 09:30 AM through November 30, 2025, 20:23:00.
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
        - Maintains the **10 most recent calls** in `lucashou_fire911_recent` (rows `pos00`–`pos09`).

- **Serving layer & web app (HBase REST + Node.js/Express)**
    - A Node.js web app talks to HBase via the REST gateway on the EMR primary node.
    - It joins:
        - **Batch counts** from `lucashou_fire_calls_by_type`
        - **Speed-layer deltas** from `lucashou_fire_calls_by_type_speed`
        - **Recent incidents** from `lucashou_fire911_recent`
    - It exposes:
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

## 3. Prerequisites

- Access to the course EMR cluster with HDFS, Hive, HBase, and Spark configured.
- Access to the course Kafka cluster plus `kafka.client.properties` file.
- Java and Maven installed on local machine and cluster to build/run the producer and Spark job.
- Node.js and npm installed on the machine hosting the web app.
- Socrata API credentials for the Seattle 911 API, set via environment variables on the producer host.
- The HBase REST server enabled and reachable from the web app host.

## 4. End-to-End Workflow

1. Upload the historical Seattle 911 CSV into HDFS.  
2. Run the Hive script to create the external CSV table and the cleaned ORC table with derived time fields.  
3. Run the Hive batch-aggregation script to build the per–call-type summary table.  
4. Create the HBase table for batch summaries and run the Hive HBase script to load the batch aggregates into it.  
5. Build the Kafka producer jar with Maven, transfer uberjar onto cluster, and configure environment variables (API keys).  
6. Start the Kafka producer so it periodically polls the Seattle 911 API and publishes new incidents to the Kafka topic.  
7. Create the three speed-layer HBase tables for seen incidents, per-type speed counts, and the 10 most recent calls.  
8. Build the Spark Streaming speed-layer jar, transfer onto cluster, and submit the streaming job to consume from Kafka and update the HBase speed tables.
9. Install Node.js dependencies for the web app, start the Express app and open the dashboard URL in a browser.

## 5. Limitations and Future Work

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