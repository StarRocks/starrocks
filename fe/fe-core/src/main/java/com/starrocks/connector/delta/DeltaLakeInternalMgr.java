// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.delta;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.common.Config;
import com.starrocks.connector.ReentrantExecutor;
import com.starrocks.external.hive.CachingHiveMetastore;
import com.starrocks.external.hive.CachingHiveMetastoreConf;
import com.starrocks.external.hive.HiveMetaClient;
import com.starrocks.external.hive.HiveMetastore;
import com.starrocks.external.hive.IHiveMetastore;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.hive.HiveConnector.HIVE_METASTORE_URIS;

public class DeltaLakeInternalMgr {
    private final String catalogName;
    private final Map<String, String> properties;
    private final boolean enableMetastoreCache;
    private final CachingHiveMetastoreConf hmsConf;
    private ExecutorService refreshHiveMetastoreExecutor;

    public DeltaLakeInternalMgr(String catalogName, Map<String, String> properties) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.enableMetastoreCache = Boolean.parseBoolean(properties.getOrDefault("enable_metastore_cache", "false"));
        this.hmsConf = new CachingHiveMetastoreConf(properties);
    }

    public IHiveMetastore createHiveMetastore() {
        // TODO(stephen): Abstract the creator class to construct hive meta client
        HiveMetaClient metaClient = createHiveMetaClient();
        IHiveMetastore hiveMetastore = new HiveMetastore(metaClient, catalogName);
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

    public HiveMetaClient createHiveMetaClient() {
        HiveConf conf = new HiveConf();
        conf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), properties.get(HIVE_METASTORE_URIS));
        conf.set(MetastoreConf.ConfVars.CLIENT_SOCKET_TIMEOUT.getHiveName(),
                String.valueOf(Config.hive_meta_store_timeout_s));
        return new HiveMetaClient(conf);
    }

    public CachingHiveMetastoreConf getHiveMetastoreConf() {
        return hmsConf;
    }
}
