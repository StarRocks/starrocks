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

import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.glue.IcebergGlueCatalog;
import com.starrocks.connector.iceberg.hadoop.IcebergHadoopCatalog;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.connector.iceberg.jdbc.IcebergJdbcCatalog;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.util.ThreadPools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;
import static org.apache.iceberg.util.ThreadPools.newWorkerPool;

public class IcebergConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(IcebergConnector.class);
    private final Map<String, String> properties;
    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private IcebergCatalog icebergNativeCatalog;
    private ExecutorService icebergJobPlanningExecutor;
    private ExecutorService refreshOtherFeExecutor;
    private final IcebergCatalogProperties icebergCatalogProperties;
    private final ConnectorProperties connectorProperties;

    public IcebergConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
        this.icebergCatalogProperties = new IcebergCatalogProperties(properties);
        this.connectorProperties = new ConnectorProperties(ConnectorType.ICEBERG, properties);
    }

    private IcebergCatalog buildIcebergNativeCatalog() {
        IcebergCatalogType nativeCatalogType = icebergCatalogProperties.getCatalogType();
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
            case JDBC_CATALOG:
                return new IcebergJdbcCatalog(catalogName, conf, properties);
            default:
                throw new StarRocksConnectorException("Property %s is missing or not supported now.", ICEBERG_CATALOG_TYPE);
        }
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new IcebergMetadata(catalogName, hdfsEnvironment, getNativeCatalog(),
                buildIcebergJobPlanningExecutor(), buildRefreshOtherFeExecutor(), icebergCatalogProperties, connectorProperties);
    }

    // In order to be compatible with the catalog created with the wrong configuration,
    // icebergNativeCatalog is lazy, mainly to prevent fe restart failure.
    public IcebergCatalog getNativeCatalog() {
        if (icebergNativeCatalog == null) {
            IcebergCatalog nativeCatalog = buildIcebergNativeCatalog();

            if (icebergCatalogProperties.enableIcebergMetadataCache() && !isResourceMappingCatalog(catalogName)) {
                nativeCatalog = new CachingIcebergCatalog(catalogName, nativeCatalog,
                        icebergCatalogProperties, buildBackgroundJobPlanningExecutor());
                GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor()
                        .registerCachingIcebergCatalog(catalogName, nativeCatalog);
            }
            this.icebergNativeCatalog = nativeCatalog;
        }
        return icebergNativeCatalog;
    }

    private ExecutorService buildIcebergJobPlanningExecutor() {
        if (icebergJobPlanningExecutor == null) {
            icebergJobPlanningExecutor = newWorkerPool(catalogName + "-sr-iceberg-worker-pool",
                    icebergCatalogProperties.getIcebergJobPlanningThreadNum());
        }

        return icebergJobPlanningExecutor;
    }

    public ExecutorService buildRefreshOtherFeExecutor() {
        if (refreshOtherFeExecutor == null) {
            refreshOtherFeExecutor = newWorkerPool(catalogName + "-refresh-others-fe-iceberg-metadata-cache",
                    icebergCatalogProperties.getRefreshOtherFeIcebergCacheThreadNum());
        }
        return refreshOtherFeExecutor;
    }

    private ExecutorService buildBackgroundJobPlanningExecutor() {
        return newWorkerPool(catalogName + "-background-iceberg-worker-pool",
                icebergCatalogProperties.getBackgroundIcebergJobPlanningThreadNum());
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
}
