// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.connector.iceberg;

public enum IcebergCatalogType {
    HIVE_CATALOG,
    CUSTOM_CATALOG,
    GLUE_CATALOG,
    REST_CATALOG,
    HADOOP_CATALOG,
    JDBC_CATALOG,

    UNKNOWN;
    // TODO: add more iceberg catalog type

    /**
     * Which external system a catalog of this type contacts, as
     * {@link com.starrocks.common.util.concurrent.lock.BlockingCallValidator} reports it. The tag is
     * what makes "lock-held time grouped by transport" aggregatable, so the same system must always
     * get the same one: a hadoop catalog is a directory tree, so it is storage, and a hive catalog
     * reaches the metastore through {@code HiveMetaClient}, which tags its own requests.
     */
    public String transportTag() {
        switch (this) {
            case GLUE_CATALOG:
                return "iceberg-glue";
            case REST_CATALOG:
                return "iceberg-rest";
            case JDBC_CATALOG:
                return "iceberg-jdbc";
            case HADOOP_CATALOG:
                return "remote-storage";
            case HIVE_CATALOG:
                return "hive-metastore";
            default:
                return "iceberg-catalog";
        }
    }

    public static IcebergCatalogType fromString(String catalogType) {
        for (IcebergCatalogType type : IcebergCatalogType.values()) {
            if (type.name().equalsIgnoreCase(String.format("%s_CATALOG", catalogType))) {
                return type;
            }
        }
        return UNKNOWN;
    }
}
