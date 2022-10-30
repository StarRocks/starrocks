// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.iceberg;

public enum IcebergCatalogType {
    HIVE_CATALOG,
    CUSTOM_CATALOG,
    GLUE_CATALOG,
    UNKNOWN;
    // TODO: add more iceberg catalog type

    public static IcebergCatalogType fromString(String catalogType) {
        for (IcebergCatalogType type : IcebergCatalogType.values()) {
            if (type.name().equalsIgnoreCase(String.format("%s_CATALOG", catalogType))) {
                return type;
            }
        }
        return UNKNOWN;
    }
}
