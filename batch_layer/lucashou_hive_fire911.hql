-- External table over the raw CSV in HDFS
CREATE EXTERNAL TABLE IF NOT EXISTS lucashou_fire911_csv (
                                                             address         string,
                                                             type            string,
                                                             datetime_str    string,
                                                             latitude        double,
                                                             longitude       double,
                                                             report_location string,
                                                             incident_number string
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
        "separatorChar" = ",",
        "quoteChar"     = "\""
        )
    STORED AS TEXTFILE
    LOCATION '/lucashou/seattle_fire_911';

-- ORC table with parsed timestamp and derived fields
CREATE TABLE IF NOT EXISTS lucashou_fire911 (
                                                type            string,
                                                datetime        timestamp,
                                                latitude        double,
                                                longitude       double,
                                                incident_number string,
                                                hour_of_day     tinyint,
                                                day_of_week     tinyint
)
    STORED AS ORC;

INSERT OVERWRITE TABLE lucashou_fire911
SELECT
    type,
    CAST(
            from_unixtime(
                    unix_timestamp(datetime_str, 'yyyy MMM dd hh:mm:ss a')
            ) AS timestamp
    )                         AS datetime,
    latitude,
    longitude,
    incident_number,
    hour(
            from_unixtime(
                    unix_timestamp(datetime_str, 'yyyy MMM dd hh:mm:ss a')
            )
    )                         AS hour_of_day,
    dayofweek(
            from_unixtime(
                    unix_timestamp(datetime_str, 'yyyy MMM dd hh:mm:ss a')
            )
    )                         AS day_of_week
FROM lucashou_fire911_csv
WHERE datetime_str IS NOT NULL;