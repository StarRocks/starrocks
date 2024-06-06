// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.connector.delta;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.ReentrantExecutor;
import com.starrocks.connector.delta.CachingDeltaLakeMetastore;
import com.starrocks.connector.delta.DeltaLakeInternalMgr;
import com.starrocks.connector.metastore.IMetastore;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.starrocks.epack.connector.delta.DatabricksUnityMetastore.DATABRICKS_CATALOG_NAME;
import static com.starrocks.epack.connector.delta.DatabricksUnityMetastore.DATABRICKS_HOST;
import static com.starrocks.epack.connector.delta.DatabricksUnityMetastore.DATABRICKS_TOKEN;

public class DeltaLakeInternalMgrEpack extends DeltaLakeInternalMgr {
    private static final List<String> SUPPORTED_METASTORE_TYPE = ImmutableList.of("hive", "glue", "dlf", "unity");
    public DeltaLakeInternalMgrEpack(String catalogName, Map<String, String> properties,
                                     HdfsEnvironment hdfsEnvironment) {
        super(catalogName, properties, hdfsEnvironment);
    }

    @Override
    public boolean isSupportedMetastoreType(String metastoreType) {
        return SUPPORTED_METASTORE_TYPE.contains(metastoreType);
    }

    @Override
    public IMetastore createDeltaLakeMetastore() {
        if (metastoreType == MetastoreType.UNITY) {
            return createUnityBackedDeltaLakeMetastore();
        } else {
            return createHMSBackedDeltaLakeMetastore();
        }
    }

    public IMetastore createUnityBackedDeltaLakeMetastore() {
        if (!properties.containsKey(DATABRICKS_HOST) || !properties.containsKey(DATABRICKS_TOKEN)) {
            throw new IllegalArgumentException("Databricks host and token must be set");
        }
        if (!properties.containsKey(DATABRICKS_CATALOG_NAME)) {
            throw new IllegalArgumentException("Databricks catalog name must be set");
        }
        String host = properties.get(DATABRICKS_HOST);
        String token = properties.get(DATABRICKS_TOKEN);
        String dataBricksCatalogName = properties.get(DATABRICKS_CATALOG_NAME);
        DatabricksConfig cfg = new DatabricksConfig().setHost(host).setToken(token);
        WorkspaceClient client = new WorkspaceClient(cfg);
        DatabricksUnityMetastore databricksUnityMetastore = new DatabricksUnityMetastore(catalogName,
                dataBricksCatalogName, client, hdfsEnvironment);
        UnityBackedDeltaLakeMetastore unityBackedDeltaLakeMetastore =
                new UnityBackedDeltaLakeMetastore(catalogName, databricksUnityMetastore, hdfsEnvironment.getConfiguration());

        IMetastore deltaLakeMetastore;
        if (!enableMetastoreCache) {
            deltaLakeMetastore = unityBackedDeltaLakeMetastore;
        } else {
            refreshHiveMetastoreExecutor = Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder().setNameFormat("deltalake-metastore-refresh-%d").build());
            Executor executor = new ReentrantExecutor(refreshHiveMetastoreExecutor, hmsConf.getCacheRefreshThreadMaxNum());
            deltaLakeMetastore = CachingDeltaLakeMetastore.createCatalogLevelInstance(unityBackedDeltaLakeMetastore, executor,
                    hmsConf.getCacheTtlSec(), hmsConf.getCacheRefreshIntervalSec(), hmsConf.getCacheMaxNum());
        }

        return deltaLakeMetastore;
    }
}
