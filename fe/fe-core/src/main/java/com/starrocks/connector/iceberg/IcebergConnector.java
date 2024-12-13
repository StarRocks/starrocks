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

<<<<<<< HEAD
import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
=======
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
=======
import java.util.List;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

<<<<<<< HEAD
=======
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;
import static org.apache.iceberg.util.ThreadPools.newWorkerPool;

public class IcebergConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(IcebergConnector.class);
<<<<<<< HEAD
    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    @Deprecated
    public static final String ICEBERG_CATALOG_LEGACY = "starrocks.catalog-type";
    @Deprecated
    public static final String ICEBERG_METASTORE_URIS = "iceberg.catalog.hive.metastore.uris";
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final String HIVE_METASTORE_TIMEOUT = "hive.metastore.timeout";
    public static final String ICEBERG_CUSTOM_PROPERTIES_PREFIX = "iceberg.catalog.";
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    private final Map<String, String> properties;
    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private IcebergCatalog icebergNativeCatalog;
    private ExecutorService icebergJobPlanningExecutor;
    private ExecutorService refreshOtherFeExecutor;
<<<<<<< HEAD
=======
    private final IcebergCatalogProperties icebergCatalogProperties;
    private final ConnectorProperties connectorProperties;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    public IcebergConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
<<<<<<< HEAD
    }

    private IcebergCatalog buildIcebergNativeCatalog() {
        IcebergCatalogType nativeCatalogType = getNativeCatalogType();
=======
        this.icebergCatalogProperties = new IcebergCatalogProperties(properties);
        this.connectorProperties = new ConnectorProperties(ConnectorType.ICEBERG, properties);
    }

    private IcebergCatalog buildIcebergNativeCatalog() {
        IcebergCatalogType nativeCatalogType = icebergCatalogProperties.getCatalogType();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
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
=======
    @Override
    public ConnectorMetadata getMetadata() {
        return new IcebergMetadata(catalogName, hdfsEnvironment, getNativeCatalog(),
                buildIcebergJobPlanningExecutor(), buildRefreshOtherFeExecutor(), icebergCatalogProperties, connectorProperties);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    // In order to be compatible with the catalog created with the wrong configuration,
    // icebergNativeCatalog is lazy, mainly to prevent fe restart failure.
    public IcebergCatalog getNativeCatalog() {
        if (icebergNativeCatalog == null) {
            IcebergCatalog nativeCatalog = buildIcebergNativeCatalog();
<<<<<<< HEAD
            boolean enableMetadataCache;
            if (properties.containsKey("enable_iceberg_metadata_cache")) {
                enableMetadataCache = Boolean.parseBoolean(properties.get("enable_iceberg_metadata_cache"));
            } else {
                enableMetadataCache = getNativeCatalogType() == IcebergCatalogType.GLUE_CATALOG;
            }

            if (enableMetadataCache && !isResourceMappingCatalog(catalogName)) {
                long ttl = Long.parseLong(properties.getOrDefault("iceberg_meta_cache_ttl_sec", "1800"));
                nativeCatalog = new CachingIcebergCatalog(nativeCatalog, ttl, buildBackgroundJobPlanningExecutor());
=======

            if (icebergCatalogProperties.enableIcebergMetadataCache() && !isResourceMappingCatalog(catalogName)) {
                nativeCatalog = new CachingIcebergCatalog(catalogName, nativeCatalog,
                        icebergCatalogProperties, buildBackgroundJobPlanningExecutor());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor()
                        .registerCachingIcebergCatalog(catalogName, nativeCatalog);
            }
            this.icebergNativeCatalog = nativeCatalog;
        }
        return icebergNativeCatalog;
    }

    private ExecutorService buildIcebergJobPlanningExecutor() {
        if (icebergJobPlanningExecutor == null) {
<<<<<<< HEAD
            int poolSize = Math.max(2, Integer.parseInt(properties.getOrDefault("iceberg_job_planning_thread_num",
                    String.valueOf(Config.iceberg_worker_num_threads))));
            icebergJobPlanningExecutor = newWorkerPool(catalogName + "-sr-iceberg-worker-pool", poolSize);
=======
            icebergJobPlanningExecutor = newWorkerPool(catalogName + "-sr-iceberg-worker-pool",
                    icebergCatalogProperties.getIcebergJobPlanningThreadNum());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }

        return icebergJobPlanningExecutor;
    }

    public ExecutorService buildRefreshOtherFeExecutor() {
        if (refreshOtherFeExecutor == null) {
<<<<<<< HEAD
            int threadSize = Math.max(2, Integer.parseInt(
                    properties.getOrDefault("refresh-other-fe-iceberg-cache-thread-num", "4")));
            refreshOtherFeExecutor = newWorkerPool(catalogName + "-refresh-others-fe-iceberg-metadata-cache", threadSize);
=======
            refreshOtherFeExecutor = newWorkerPool(catalogName + "-refresh-others-fe-iceberg-metadata-cache",
                    icebergCatalogProperties.getRefreshOtherFeIcebergCacheThreadNum());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
        return refreshOtherFeExecutor;
    }

    private ExecutorService buildBackgroundJobPlanningExecutor() {
<<<<<<< HEAD
        int defaultPoolSize = Math.max(2, Runtime.getRuntime().availableProcessors() / 8);
        int backgroundIcebergJobPlanningThreadPoolSize = Integer.parseInt(properties.getOrDefault(
                "background_iceberg_job_planning_thread_num", String.valueOf(defaultPoolSize)));
        return newWorkerPool(catalogName + "-background-iceberg-worker-pool", backgroundIcebergJobPlanningThreadPoolSize);
=======
        return newWorkerPool(catalogName + "-background-iceberg-worker-pool",
                icebergCatalogProperties.getBackgroundIcebergJobPlanningThreadNum());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
=======

    @Override
    public boolean supportMemoryTrack() {
        return icebergCatalogProperties.enableIcebergMetadataCache() && icebergNativeCatalog != null;
    }

    @Override
    public Map<String, Long> estimateCount() {
        return icebergNativeCatalog.estimateCount();
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        return icebergNativeCatalog.getSamples();
    }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
