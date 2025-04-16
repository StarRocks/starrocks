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
import com.starrocks.common.util.Util;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.ReentrantExecutor;
import com.starrocks.connector.hive.CachingHiveMetastoreConf;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.hive.HiveConnector.HIVE_METASTORE_TYPE;
import static com.starrocks.connector.hive.HiveConnector.HIVE_METASTORE_URIS;

public class DeltaLakeInternalMgr {
    public static final List<String> SUPPORTED_METASTORE_TYPE = ImmutableList.of("hive", "glue", "dlf");
    protected final String catalogName;
    protected final DeltaLakeCatalogProperties deltaLakeCatalogProperties;
    protected final HdfsEnvironment hdfsEnvironment;
    private final CachingHiveMetastoreConf hmsConf;
    private ExecutorService refreshHiveMetastoreExecutor;
    protected final MetastoreType metastoreType;

    public DeltaLakeInternalMgr(String catalogName, Map<String, String> properties, HdfsEnvironment hdfsEnvironment) {
        this.catalogName = catalogName;
        this.deltaLakeCatalogProperties = new DeltaLakeCatalogProperties(properties);
        this.hmsConf = new CachingHiveMetastoreConf(properties, "delta lake");
        this.hdfsEnvironment = hdfsEnvironment;

        String hiveMetastoreType = properties.getOrDefault(HIVE_METASTORE_TYPE, "hive").toLowerCase();
        if (!isSupportedMetastoreType(hiveMetastoreType)) {
            throw new SemanticException("hive metastore type [%s] is not supported", hiveMetastoreType);
        }

        if (hiveMetastoreType.equals("hive")) {
            String hiveMetastoreUris = Preconditions.checkNotNull(properties.get(HIVE_METASTORE_URIS),
                    "%s must be set in properties when creating catalog of hive-metastore", HIVE_METASTORE_URIS);
            Util.validateMetastoreUris(hiveMetastoreUris);
        }
        this.metastoreType = MetastoreType.get(hiveMetastoreType);
    }

    protected boolean isSupportedMetastoreType(String metastoreType) {
        return SUPPORTED_METASTORE_TYPE.contains(metastoreType);
    }

    public IDeltaLakeMetastore createDeltaLakeMetastore() {
        return createHMSBackedDeltaLakeMetastore();
    }

    public IDeltaLakeMetastore createHMSBackedDeltaLakeMetastore() {
        HiveMetaClient metaClient = HiveMetaClient.createHiveMetaClient(hdfsEnvironment,
                deltaLakeCatalogProperties.getProperties());
        IHiveMetastore hiveMetastore = new HiveMetastore(metaClient, catalogName, metastoreType);
        HMSBackedDeltaMetastore hmsBackedDeltaMetastore = new HMSBackedDeltaMetastore(catalogName, hiveMetastore,
                hdfsEnvironment.getConfiguration(), deltaLakeCatalogProperties);
        IDeltaLakeMetastore deltaLakeMetastore;
        if (!deltaLakeCatalogProperties.isEnableDeltaLakeTableCache()) {
            deltaLakeMetastore = hmsBackedDeltaMetastore;
        } else {
            refreshHiveMetastoreExecutor = Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder().setNameFormat("deltalake-metastore-refresh-%d").build());
            Executor executor = new ReentrantExecutor(refreshHiveMetastoreExecutor, hmsConf.getCacheRefreshThreadMaxNum());
            deltaLakeMetastore = CachingDeltaLakeMetastore.createCatalogLevelInstance(hmsBackedDeltaMetastore, executor,
                    hmsConf.getCacheTtlSec(), hmsConf.getCacheRefreshIntervalSec(), hmsConf.getCacheMaxNum());
        }

        return deltaLakeMetastore;
    }

    public void shutdown() {
        if (deltaLakeCatalogProperties.isEnableDeltaLakeTableCache() && refreshHiveMetastoreExecutor != null) {
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
