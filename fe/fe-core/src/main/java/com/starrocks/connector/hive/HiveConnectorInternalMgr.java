// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.common.Config;
import com.starrocks.external.CachingRemoteFileConf;
import com.starrocks.external.CachingRemoteFileIO;
import com.starrocks.external.RemoteFileIO;
import com.starrocks.external.hive.CachingHiveMetastore;
import com.starrocks.external.hive.CachingHiveMetastoreConf;
import com.starrocks.external.hive.HiveMetaClient;
import com.starrocks.external.hive.HiveMetastore;
import com.starrocks.external.hive.HiveRemoteFileIO;
import com.starrocks.external.hive.IHiveMetastore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.hive.HiveConnector.HIVE_METASTORE_URIS;

public class HiveConnectorInternalMgr {
    private final String catalogName;
    private final Map<String, String> properties;
    private final boolean enableMetastoreCache;
    private CachingHiveMetastoreConf hmsConf;

    private final boolean enableRemoteFileCache;
    private CachingRemoteFileConf remoteFileConf;

    private ExecutorService hmsExecutorService;
    private ExecutorService remoteFileExecutorService;

    public HiveConnectorInternalMgr(String catalogName, Map<String, String> properties) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.enableMetastoreCache = Boolean.parseBoolean(properties.getOrDefault("enable_metastore_cache", "true"));
        if (enableMetastoreCache) {
            hmsConf = new CachingHiveMetastoreConf(properties);
        }

        this.enableRemoteFileCache = Boolean.parseBoolean(properties.getOrDefault("enable_remote_file_cache", "true"));
        if (enableRemoteFileCache) {
            remoteFileConf = new CachingRemoteFileConf(properties);
        }
    }

    public void shutdown() {
        if (enableMetastoreCache) {
            hmsExecutorService.shutdown();
        }
        if (enableRemoteFileCache) {
            remoteFileExecutorService.shutdown();
        }
    }

    public RemoteFileIO createRemoteFileIO() {
        // TODO(stephen): Abstract the creator class to construct RemoteFiloIO
        Configuration configuration = new Configuration();
        RemoteFileIO remoteFileIO = new HiveRemoteFileIO(configuration);

        RemoteFileIO baseRemoteFileIO;
        if (!enableRemoteFileCache) {
            baseRemoteFileIO = remoteFileIO;
        } else {
            remoteFileExecutorService = Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder().setNameFormat("hive-remote-refresh-%d").build());
            baseRemoteFileIO = CachingRemoteFileIO.createCatalogLevelInstance(
                    remoteFileIO,
                    remoteFileExecutorService,
                    remoteFileConf.getCacheTtlSec(),
                    remoteFileConf.getCacheRefreshIntervalSec(),
                    remoteFileConf.getCacheMaxSize());
        }
        return baseRemoteFileIO;
    }

    public IHiveMetastore createHiveMetastore() {
        // TODO(stephen): Abstract the creator class to construct hive meta client
        HiveMetaClient metaClient = createHiveMetaClient();
        IHiveMetastore hiveMetastore = new HiveMetastore(metaClient, catalogName);
        IHiveMetastore baseHiveMetastore;
        if (!enableMetastoreCache) {
            baseHiveMetastore = hiveMetastore;
        } else {
            hmsExecutorService = Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder().setNameFormat("hive-metastore-refresh-%d").build());
            baseHiveMetastore = CachingHiveMetastore.createCatalogLevelInstance(
                    hiveMetastore,
                    hmsExecutorService,
                    hmsConf.getCacheTtlSec(),
                    hmsConf.getCacheRefreshIntervalSec(),
                    hmsConf.getCacheMaxNum(),
                    hmsConf.enableListNamesCache());
        }

        return baseHiveMetastore;
    }

    public HiveMetaClient createHiveMetaClient() {
        HiveConf conf = new HiveConf();
        conf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), properties.get(HIVE_METASTORE_URIS));
        conf.set(MetastoreConf.ConfVars.CLIENT_SOCKET_TIMEOUT.getHiveName(),
                String.valueOf(Config.hive_meta_store_timeout_s));
        return new HiveMetaClient(conf);
    }
}
