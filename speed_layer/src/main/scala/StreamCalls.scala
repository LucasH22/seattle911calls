import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

import java.io.FileInputStream
import java.util.Properties
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import java.time.{LocalDateTime, ZoneId}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.streaming.dstream.DStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Increment}
import org.apache.hadoop.hbase.util.Bytes

case class FireCall(
                     incidentNumber: String,
                     callType: String,
                     period: String,
                     datetime: String,
                     tsMillis: Long,
                     latitude: String,
                     longitude: String
                   )

object StreamCalls {

  // Mapper for JSON -> FireCall fields
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  // HBase configuration and tables
  val hbaseConf: Configuration = HBaseConfiguration.create()
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val seenTable = hbaseConnection.getTable(TableName.valueOf("lucashou_fire911_seen"))
  val callsTable = hbaseConnection.getTable(TableName.valueOf("lucashou_fire_calls_by_type_speed_v2"))
  val recentTable = hbaseConnection.getTable(TableName.valueOf("lucashou_fire911_recent_v2"))

  val SeattleZone: ZoneId = ZoneId.of("America/Los_Angeles")

  def updateSpeedLayer(call: FireCall): String = {
    try {
      val incidentRow = Bytes.toBytes(call.incidentNumber)

      // Check if we've seen this incident before
      val get = new Get(incidentRow)
      val res = seenTable.get(get)

      if (!res.isEmpty) {
        s"Skipping already-seen incident ${call.incidentNumber}"
      } else {
        // Mark incident as seen
        val put = new Put(incidentRow)
        put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("seen"), Bytes.toBytes(1L))
        seenTable.put(put)

        // Increment counters by callType + period
        val inc = new Increment(Bytes.toBytes(call.callType))
        inc.addColumn(Bytes.toBytes("calls"), Bytes.toBytes(call.period), 1L)
        callsTable.increment(inc)

        s"Updated speed layer for incident ${call.incidentNumber} (${call.callType}, ${call.period})"
      }
    } catch {
      case NonFatal(e) =>
        s"ERROR updating HBase for incident ${call.incidentNumber}: ${e.getMessage}"
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        """
          |Usage: StreamCalls <brokers>
          |  <brokers> is a list of one or more Kafka brokers
          |""".stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // 2-second micro-batch interval
    val sparkConf = new SparkConf().setAppName("StreamCalls")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Kafka topic with 911 data
    val topicsSet = Set("mpcs53014_lucashou_fire911_v2")

    // Load the Kafka client properties
    val props = new Properties()
    props.load(new FileInputStream("/home/hadoop/kafka.client.properties"))

    // Convert java.util.Properties -> Map[String, Object]
    val baseParams: Map[String, Object] =
      props.asScala.map { case (k, v) => k -> v.asInstanceOf[Object] }.toMap

    // Setting auto.offset.reset to "earliest" to not miss any new entries when restarting Spark streaming
    val kafkaParams: Map[String, Object] = baseParams ++ Map(
      "bootstrap.servers"  -> brokers,
      "key.deserializer"   -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id"           -> "lucashou_fire911_speed_final",
      "auto.offset.reset"  -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Raw JSON from Kafka
    val serializedRecords: DStream[String] = stream.map(_.value())

    // DEBUG: still show JSON (first 10 per batch)
    serializedRecords.print()

    // Parse JSON into FireCall(incidentNumber, callType, period, datetime, tsMillis, lat, lon)
    val calls: DStream[FireCall] = serializedRecords.flatMap { rec =>
      try {
        val node = mapper.readTree(rec)

        val incidentOpt = Option(node.path("incident_number").asText(null))
        val callType    = Option(node.path("type").asText(null)).getOrElse("UNKNOWN")
        val datetimeOpt = Option(node.path("datetime").asText(null))
        val latOpt      = Option(node.path("latitude").asText(null))
        val lonOpt      = Option(node.path("longitude").asText(null))

        (incidentOpt, datetimeOpt, latOpt, lonOpt) match {
          case (Some(id), Some(dt), Some(lat), Some(lon)) =>
            val ldt   = LocalDateTime.parse(dt)
            val zoned = ldt.atZone(SeattleZone)
            val hour  = zoned.getHour
            val period =
              if (hour >= 8 && hour < 20) "day" else "night"

            val millis = zoned.toInstant.toEpochMilli

            Some(FireCall(id, callType, period, dt, millis, lat, lon))

          case _ =>
            // Missing required fields → drop this record
            None
        }
      } catch {
        case NonFatal(_) =>
          // Uncomment for debugging:
          // println(s"ERROR parsing JSON: ${e.getMessage}")
          None
      }
    }

    // DEBUG: show parsed FireCall objects (first 10 per batch)
    calls.print()

    // ----- HBase speed layer update + recent-10 update -----
    calls.foreachRDD { rdd =>
      val batchSize = rdd.count()
      println(s"### BATCH SIZE (FireCalls) = $batchSize")

      // 1) Update speed-layer + dedup
      rdd.foreach { call =>
        val msg = updateSpeedLayer(call)
        println(msg)
      }

      // 2) Maintain global "10 most recent calls" across batches
      if (batchSize > 0) {

        // Top 10 from this batch
        val newTop: Array[FireCall] =
          rdd.sortBy(_.tsMillis, ascending = false).take(10)

        // Read existing pos00..pos09 from HBase
        import scala.collection.mutable.ListBuffer
        val existing = ListBuffer[FireCall]()

        for (i <- 0 until 10) {
          val rowKey = f"pos$i%02d"
          val get    = new Get(Bytes.toBytes(rowKey))
          val res    = recentTable.get(get)

          if (!res.isEmpty) {
            val bytesLoc = Bytes.toBytes("loc")

            val incident = Option(res.getValue(bytesLoc, Bytes.toBytes("incident_number")))
              .map(Bytes.toString).getOrElse("")
            val ctype    = Option(res.getValue(bytesLoc, Bytes.toBytes("type")))
              .map(Bytes.toString).getOrElse("UNKNOWN")
            val dt       = Option(res.getValue(bytesLoc, Bytes.toBytes("datetime")))
              .map(Bytes.toString).getOrElse("")
            val lat      = Option(res.getValue(bytesLoc, Bytes.toBytes("lat")))
              .map(Bytes.toString).getOrElse("")
            val lon      = Option(res.getValue(bytesLoc, Bytes.toBytes("lon")))
              .map(Bytes.toString).getOrElse("")

            if (incident.nonEmpty && dt.nonEmpty) {
              val ldt   = LocalDateTime.parse(dt)
              val zoned = ldt.atZone(SeattleZone)
              val hour  = zoned.getHour
              val period = if (hour >= 8 && hour < 20) "day" else "night"

              existing += FireCall(
                incident,
                ctype,
                period,
                dt,
                zoned.toInstant.toEpochMilli,
                lat,
                lon
              )
            }
          }
        }

        // Merge existing + new, dedup by incident, sort by timestamp desc, keep 10
        val mergedTop10: Seq[FireCall] =
          (newTop.toSeq ++ existing)
            .groupBy(_.incidentNumber)
            .values
            .map(_.maxBy(_.tsMillis))
            .toSeq
            .sortBy(_.tsMillis)(Ordering.Long.reverse)
            .take(10)

        import scala.collection.JavaConverters._
        val puts = mergedTop10.zipWithIndex.map { case (c, idx) =>
          val rowKey = f"pos$idx%02d"
          val p = new Put(Bytes.toBytes(rowKey))
          p.addColumn(Bytes.toBytes("loc"), Bytes.toBytes("incident_number"), Bytes.toBytes(c.incidentNumber))
          p.addColumn(Bytes.toBytes("loc"), Bytes.toBytes("type"),            Bytes.toBytes(c.callType))
          p.addColumn(Bytes.toBytes("loc"), Bytes.toBytes("datetime"),        Bytes.toBytes(c.datetime))
          p.addColumn(Bytes.toBytes("loc"), Bytes.toBytes("lat"),             Bytes.toBytes(c.latitude))
          p.addColumn(Bytes.toBytes("loc"), Bytes.toBytes("lon"),             Bytes.toBytes(c.longitude))
          p
        }

        if (puts.nonEmpty) {
          recentTable.put(puts.asJava)
          println(s"### Updated lucashou_fire911_recent with ${puts.length} rows")
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}