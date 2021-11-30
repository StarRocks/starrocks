# ALTER ROUTINE LOAD

## description

This syntax is used to alter the established routine load jobs. Only jobs in PAUSED state can be altered.

Syntax:

```sql
ALTER ROUTINE LOAD FOR [db.]job_name
[job_properties]
FROM data_source
[data_source_properties]
```

1. `[db.]job_name`

    To specify the name of the job to be altered.

2. `tbl_name`

    To specify the name of the table to be loaded.

3. `job_properties`

    To specify the properties of the job to be altered. Currently only the following properties can be altered.

    1. `desired_concurrent_number`
    2. `max_error_number`
    3. `max_batch_interval`
    4. `max_batch_rows`
    5. `max_batch_size`
    6. `jsonpaths`
    7. `json_root`
    8. `strip_outer_array`
    9. `strict_mode`
    10. `timezone`

4. `data_source`

    Type of data source. This version supports：

    KAFKA

5. `data_source_properties`

    Related properties of data source. This version suports:
    1. `kafka_partitions`
    2. `kafka_offsets`
    3. Custom property，such as `property.group.id`

Note:

1. `kafka_partitions` and `kafka_offsets` can only be used to alter the offse of kafka partition to be consumed, and only the currently consumed partition can be modified. No new partition can be added.

## example

1. Alter `desired_concurrent_number` as 1.

    ```sql
    ALTER ROUTINE LOAD FOR db1.label1
    PROPERTIES
    (
        "desired_concurrent_number" = "1"
    );
    ```

2. Alter `desired_concurrent_number` as 10, modify offset of partition, and alter group id.

    ```sql
    ALTER ROUTINE LOAD FOR db1.label1
    PROPERTIES
    (
        "desired_concurrent_number" = "10"
    )
    FROM kafka
    (
        "kafka_partitions" = "0, 1, 2",
        "kafka_offsets" = "100, 200, 100",
        "property.group.id" = "new_group"
    );
    ```

## keyword

ALTER,ROUTINE,LOAD
