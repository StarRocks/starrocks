// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.connector.delta;

import com.starrocks.connector.delta.DeltaLakeMetastore;
import com.starrocks.connector.metastore.IMetastore;
import com.starrocks.connector.metastore.MetastoreTable;
import org.apache.hadoop.conf.Configuration;

public class UnityBackedDeltaLakeMetastore extends DeltaLakeMetastore {
    public UnityBackedDeltaLakeMetastore(String catalogName, IMetastore metastore, Configuration hdfsConfiguration) {
        super(catalogName, metastore, hdfsConfiguration);
    }

    @Override
    public MetastoreTable getMetastoreTable(String dbName, String tableName) {
        return this.delegate.getMetastoreTable(dbName, tableName);
    }
}