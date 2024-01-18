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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.common.util.Utils;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.ReentrantExecutor;
import com.starrocks.connector.hive.CachingHiveMetastore;
import com.starrocks.connector.hive.CachingHiveMetastoreConf;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.hive.HiveConnector.HIVE_METASTORE_TYPE;
import static com.starrocks.connector.hive.HiveConnector.HIVE_METASTORE_URIS;

public class DeltaLakeInternalMgr {
    public static final List<String> SUPPORTED_METASTORE_TYPE = ImmutableList.of("hive", "glue");
    private final String catalogName;
    private final Map<String, String> properties;
    private final HdfsEnvironment hdfsEnvironment;
    private final boolean enableMetastoreCache;
    private final CachingHiveMetastoreConf hmsConf;
    private ExecutorService refreshHiveMetastoreExecutor;
    private final MetastoreType metastoreType;

    public DeltaLakeInternalMgr(String catalogName, Map<String, String> properties, HdfsEnvironment hdfsEnvironment) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.enableMetastoreCache = Boolean.parseBoolean(properties.getOrDefault("enable_metastore_cache", "false"));
        this.hmsConf = new CachingHiveMetastoreConf(properties, "delta lake");
        this.hdfsEnvironment = hdfsEnvironment;

        String hiveMetastoreType = properties.getOrDefault(HIVE_METASTORE_TYPE, "hive").toLowerCase();
        if (!SUPPORTED_METASTORE_TYPE.contains(hiveMetastoreType)) {
            throw new SemanticException("hive metastore type [%s] is not supported", hiveMetastoreType);
        }

        if (hiveMetastoreType.equals("hive")) {
            String hiveMetastoreUris = Preconditions.checkNotNull(properties.get(HIVE_METASTORE_URIS),
                    "%s must be set in properties when creating catalog of hive-metastore", HIVE_METASTORE_URIS);
            Utils.validateMetastoreUris(hiveMetastoreUris);
        }
        this.metastoreType = MetastoreType.get(hiveMetastoreType);
    }

    public IHiveMetastore createHiveMetastore() {
        // TODO(stephen): Abstract the creator class to construct hive meta client
        HiveMetaClient metaClient = HiveMetaClient.createHiveMetaClient(hdfsEnvironment, properties);
        IHiveMetastore hiveMetastore = new HiveMetastore(metaClient, catalogName, metastoreType);
        IHiveMetastore baseHiveMetastore;
        if (!enableMetastoreCache) {
            baseHiveMetastore = hiveMetastore;
        } else {
            refreshHiveMetastoreExecutor = Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder().setNameFormat("hive-metastore-refresh-%d").build());
            baseHiveMetastore = CachingHiveMetastore.createCatalogLevelInstance(
                    hiveMetastore,
                    new ReentrantExecutor(refreshHiveMetastoreExecutor, hmsConf.getCacheRefreshThreadMaxNum()),
                    hmsConf.getCacheTtlSec(),
                    hmsConf.getCacheRefreshIntervalSec(),
                    hmsConf.getCacheMaxNum(),
                    hmsConf.enableListNamesCache());
        }

        return baseHiveMetastore;
    }

    public void shutdown() {
        if (enableMetastoreCache && refreshHiveMetastoreExecutor != null) {
            refreshHiveMetastoreExecutor.shutdown();
        }
    }

    public CachingHiveMetastoreConf getHiveMetastoreConf() {
        return hmsConf;
    }

    public HdfsEnvironment getHdfsEnvironment() {
        return this.hdfsEnvironment;
    }

    public MetastoreType getMetastoreType() {
        return metastoreType;
    }
}
