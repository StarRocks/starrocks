// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import java.util.List;

public interface HiveMetaStoreTable {
    // TODO(stephen): remove the dependencies on resource
    String getResourceName();

    String getCatalogName();

    String getDbName();

    String getTableName();

    List<String> getDataColumnNames();

    boolean isUnPartitioned();

    List<String> getPartitionColumnNames();

    List<Column> getPartitionColumns();

    String getTableLocation();
}
