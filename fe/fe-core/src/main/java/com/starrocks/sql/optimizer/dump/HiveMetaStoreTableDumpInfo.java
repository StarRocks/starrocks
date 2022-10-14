// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.dump;

import java.util.List;

public interface HiveMetaStoreTableDumpInfo {
    default String getType() {
        return "";
    }

    default void setPartitionNames(List<String> partitionNames) {
    }

    default List<String> getPartitionNames() {
        return null;
    }

    default void setPartColumnNames(List<String> partColumnNames) {
    }

    default List<String> getPartColumnNames() {
        return null;
    }

    default void setDataColumnNames(List<String> dataColumnNames) {
    }

    default List<String> getDataColumnNames() {
        return null;
    }
}
