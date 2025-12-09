-- Batch summary of 911 calls per response type
CREATE TABLE IF NOT EXISTS lucashou_fire_calls_by_type_batch (
                                                                 type         string,
                                                                 total_calls  bigint,
                                                                 night_calls  bigint,
                                                                 day_calls    bigint
)
    STORED AS ORC;

INSERT OVERWRITE TABLE lucashou_fire_calls_by_type_batch
SELECT
    type,
    COUNT(*)                                                   AS total_calls,
    COUNT(IF(hour_of_day BETWEEN 0 AND 7
                 OR hour_of_day BETWEEN 20 AND 23, 1, NULL))       AS night_calls,
    COUNT(IF(hour_of_day BETWEEN 8 AND 19, 1, NULL))           AS day_calls
FROM lucashou_fire911
GROUP BY type;