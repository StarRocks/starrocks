package com.starrocks.connector.iceberg;

public enum IcebergPartitionTransform {
    YEAR,
    MONTH,
    DAY,
    HOUR,
    BUCKET,
    TRUNCATE,
    IDENTITY,
    UNKNOWN;

    public static IcebergPartitionTransform fromString(String partitionTransform) {
        try {
            return IcebergPartitionTransform.valueOf(partitionTransform.toUpperCase());
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
