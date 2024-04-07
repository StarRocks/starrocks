// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.delta;

import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.hive.CachingHiveMetastore;
import com.starrocks.connector.hive.CachingHiveMetastoreConf;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.connector.hive.IHiveMetastore;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import java.util.Map;

import static com.starrocks.connector.delta.DeltaLakeConnector.HIVE_METASTORE_URIS;
import static com.starrocks.connector.hive.CachingHiveMetastore.createQueryLevelInstance;

public class DeltaLakeMetadataFactory {
    private final String catalogName;
    private final IHiveMetastore metastore;
    private final long perQueryMetastoreMaxNum;
    private final HdfsEnvironment hdfsEnvironment;

    public DeltaLakeMetadataFactory(String catalogName, IHiveMetastore metastore, CachingHiveMetastoreConf hmsConf,
                                    Map<String, String> properties, HdfsEnvironment hdfsEnvironment) {
        this.catalogName = catalogName;
        this.metastore = metastore;
        this.perQueryMetastoreMaxNum = hmsConf.getPerQueryCacheMaxNum();
        this.hdfsEnvironment = hdfsEnvironment;
        if (properties.containsKey(HIVE_METASTORE_URIS)) {
            this.hdfsEnvironment.getConfiguration().set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(),
                    properties.get(HIVE_METASTORE_URIS));
        }
    }

    public DeltaLakeMetadata create() {
        HiveMetastoreOperations hiveMetastoreOperations = new HiveMetastoreOperations(
                createQueryLevelInstance(metastore, perQueryMetastoreMaxNum), metastore instanceof CachingHiveMetastore);

        return new DeltaLakeMetadata(hdfsEnvironment.getConfiguration(), catalogName, hiveMetastoreOperations);
    }
}
