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

package com.starrocks.connector.iceberg;

import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.glue.IcebergGlueCatalog;
import com.starrocks.connector.iceberg.hadoop.IcebergHadoopCatalog;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.util.ThreadPools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;
import static org.apache.iceberg.util.ThreadPools.newWorkerPool;

public class IcebergConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(IcebergConnector.class);
    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    @Deprecated
    public static final String ICEBERG_CATALOG_LEGACY = "starrocks.catalog-type";
    @Deprecated
    public static final String ICEBERG_METASTORE_URIS = "iceberg.catalog.hive.metastore.uris";
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final String ICEBERG_CUSTOM_PROPERTIES_PREFIX = "iceberg.catalog.";
    private final Map<String, String> properties;
    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private IcebergCatalog icebergNativeCatalog;
    private ExecutorService icebergJobPlanningExecutor;
    private ExecutorService refreshOtherFeExecutor;

    public IcebergConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
    }

    private IcebergCatalog buildIcebergNativeCatalog() {
        IcebergCatalogType nativeCatalogType = getNativeCatalogType();
        Configuration conf = hdfsEnvironment.getConfiguration();

        if (Config.enable_iceberg_custom_worker_thread) {
            LOG.info("Default iceberg worker thread number changed " + Config.iceberg_worker_num_threads);
            Properties props = System.getProperties();
            props.setProperty(ThreadPools.WORKER_THREAD_POOL_SIZE_PROP, String.valueOf(Config.iceberg_worker_num_threads));
        }

        switch (nativeCatalogType) {
            case HIVE_CATALOG:
                return new IcebergHiveCatalog(catalogName, conf, properties);
            case GLUE_CATALOG:
                return new IcebergGlueCatalog(catalogName, conf, properties);
            case REST_CATALOG:
                return new IcebergRESTCatalog(catalogName, conf, properties);
            case HADOOP_CATALOG:
                return new IcebergHadoopCatalog(catalogName, conf, properties);
            default:
                throw new StarRocksConnectorException("Property %s is missing or not supported now.", ICEBERG_CATALOG_TYPE);
        }
    }

    private IcebergCatalogType getNativeCatalogType() {
        String nativeCatalogTypeStr = properties.get(ICEBERG_CATALOG_TYPE);
        if (Strings.isNullOrEmpty(nativeCatalogTypeStr)) {
            nativeCatalogTypeStr = properties.get(ICEBERG_CATALOG_LEGACY);
        }
        if (Strings.isNullOrEmpty(nativeCatalogTypeStr)) {
            throw new StarRocksConnectorException("Can't find iceberg native catalog type. You must specify the" +
                    " 'iceberg.catalog.type' property when creating an iceberg catalog in the catalog properties");
        }
        return IcebergCatalogType.fromString(nativeCatalogTypeStr);
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new IcebergMetadata(catalogName, hdfsEnvironment, getNativeCatalog(),
                buildIcebergJobPlanningExecutor(), buildRefreshOtherFeExecutor());
    }

    // In order to be compatible with the catalog created with the wrong configuration,
    // icebergNativeCatalog is lazy, mainly to prevent fe restart failure.
    public IcebergCatalog getNativeCatalog() {
        if (icebergNativeCatalog == null) {
            IcebergCatalog nativeCatalog = buildIcebergNativeCatalog();
            boolean enableMetadataCache = Boolean.parseBoolean(
                    properties.getOrDefault("enable_iceberg_metadata_cache", "true"));
            if (enableMetadataCache && !isResourceMappingCatalog(catalogName)) {
                long ttl = Long.parseLong(properties.getOrDefault("iceberg_meta_cache_ttl_sec", "1800"));
                nativeCatalog = new CachingIcebergCatalog(nativeCatalog, ttl, buildBackgroundJobPlanningExecutor());
                GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor()
                        .registerCachingIcebergCatalog(catalogName, nativeCatalog);
            }
            this.icebergNativeCatalog = nativeCatalog;
        }
        return icebergNativeCatalog;
    }

    private ExecutorService buildIcebergJobPlanningExecutor() {
        if (icebergJobPlanningExecutor == null) {
            int poolSize = Math.max(2, Integer.parseInt(properties.getOrDefault("iceberg_job_planning_thread_num",
                    String.valueOf(Config.iceberg_worker_num_threads))));
            icebergJobPlanningExecutor = newWorkerPool(catalogName + "-sr-iceberg-worker-pool", poolSize);
        }

        return icebergJobPlanningExecutor;
    }

    public ExecutorService buildRefreshOtherFeExecutor() {
        if (refreshOtherFeExecutor == null) {
            int threadSize = Math.max(2, Integer.parseInt(
                    properties.getOrDefault("refresh-other-fe-iceberg-cache-thread-num", "4")));
            refreshOtherFeExecutor = newWorkerPool(catalogName + "-refresh-others-fe-iceberg-metadata-cache", threadSize);
        }
        return refreshOtherFeExecutor;
    }

    private ExecutorService buildBackgroundJobPlanningExecutor() {
        int defaultPoolSize = Math.max(2, Runtime.getRuntime().availableProcessors() / 8);
        int backgroundIcebergJobPlanningThreadPoolSize = Integer.parseInt(properties.getOrDefault(
                "background_iceberg_job_planning_thread_num", String.valueOf(defaultPoolSize)));
        return newWorkerPool(catalogName + "-background-iceberg-worker-pool", backgroundIcebergJobPlanningThreadPoolSize);
    }

    @Override
    public void shutdown() {
        GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor().unRegisterCachingIcebergCatalog(catalogName);
        if (icebergJobPlanningExecutor != null) {
            icebergJobPlanningExecutor.shutdown();
        }
        if (refreshOtherFeExecutor != null) {
            refreshOtherFeExecutor.shutdown();
        }
    }
}
