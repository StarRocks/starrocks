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


package com.starrocks.connector.delta;

import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.hive.CachingHiveMetastore;
import com.starrocks.connector.hive.CachingHiveMetastoreConf;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.connector.hive.IHiveMetastore;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import static com.starrocks.connector.hive.CachingHiveMetastore.createQueryLevelInstance;

public class DeltaLakeMetadataFactory {
    private final String catalogName;
    private final IHiveMetastore metastore;
    private final long perQueryMetastoreMaxNum;
    private final HdfsEnvironment hdfsEnvironment;

    public DeltaLakeMetadataFactory(String catalogName, IHiveMetastore metastore, CachingHiveMetastoreConf hmsConf,
                                    String uri, HdfsEnvironment hdfsEnvironment) {
        this.catalogName = catalogName;
        this.metastore = metastore;
        this.perQueryMetastoreMaxNum = hmsConf.getPerQueryCacheMaxNum();
        this.hdfsEnvironment = hdfsEnvironment;
        this.hdfsEnvironment.getConfiguration().set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), uri);
    }

    public DeltaLakeMetadata create() {
        HiveMetastoreOperations hiveMetastoreOperations = new HiveMetastoreOperations(
                createQueryLevelInstance(metastore, perQueryMetastoreMaxNum),
                metastore instanceof CachingHiveMetastore,
                hdfsEnvironment.getConfiguration());

        return new DeltaLakeMetadata(hdfsEnvironment.getConfiguration(), catalogName, hiveMetastoreOperations);
    }
}
