// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.connector.delta;

import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.delta.DeltaLakeMetadataFactory;
import com.starrocks.connector.hive.CachingHiveMetastoreConf;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.connector.metastore.IMetastore;

import java.util.Map;

import static com.starrocks.connector.hive.CachingHiveMetastore.createQueryLevelInstance;

public class DeltaLakeMetadataFactoryEpack extends DeltaLakeMetadataFactory {
    public DeltaLakeMetadataFactoryEpack(String catalogName, IMetastore metastore,
                                         CachingHiveMetastoreConf hmsConf,
                                         Map<String, String> properties,
                                         HdfsEnvironment hdfsEnvironment,
                                         MetastoreType metastoreType) {
        super(catalogName, metastore, hmsConf, properties, hdfsEnvironment, metastoreType);
    }

    @Override
    protected IMetastore createQueryLevelCacheMetastore() {
        if (metastoreType == MetastoreType.UNITY) {
            // todo: implement query level cache for Unity metastore
            return metastore;
        } else {
            return createQueryLevelInstance((IHiveMetastore) metastore, perQueryMetastoreMaxNum);
        }
    }
}
