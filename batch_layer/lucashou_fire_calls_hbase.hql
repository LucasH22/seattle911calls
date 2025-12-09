CREATE EXTERNAL TABLE IF NOT EXISTS lucashou_fire_calls_by_type_hbase (
                                                                          type         string,
                                                                          total_calls  bigint,
                                                                          night_calls  bigint,
                                                                          day_calls    bigint
)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
        WITH SERDEPROPERTIES (
        "hbase.columns.mapping" = ":key,calls:total_calls,calls:night_calls,calls:day_calls"
        )
    TBLPROPERTIES (
        "hbase.table.name" = "lucashou_fire_calls_by_type"
        );

INSERT OVERWRITE TABLE lucashou_fire_calls_by_type_hbase
SELECT * FROM lucashou_fire_calls_by_type_batch;