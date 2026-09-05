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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;
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
import com.starrocks.connector.iceberg.procedure.IcebergProcedureRegistry;
import com.starrocks.connector.iceberg.procedure.RegisterTableProcedure;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.connector.share.credential.AwsSseCUtil;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.util.ThreadPools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
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
    private final IcebergCatalogProperties icebergCatalogProperties;
    private final ConnectorProperties connectorProperties;
    private final IcebergProcedureRegistry procedureRegistry;
    // Global commit queue manager for this catalog - shared across all queries
    // to serialize commits to the same table from different queries
    private final IcebergCommitQueueManager commitQueueManager;

    public IcebergConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
        this.icebergCatalogProperties = new IcebergCatalogProperties(properties);
        this.connectorProperties = new ConnectorProperties(ConnectorType.ICEBERG, properties);
        this.procedureRegistry = new IcebergProcedureRegistry();

        // Initialize commit queue manager with a supplier that reads the latest FE configuration
        // This is a singleton per catalog, shared across all queries
        this.commitQueueManager = new IcebergCommitQueueManager(() -> {
            IcebergCommitQueueManager.Config queueConfig = new IcebergCommitQueueManager.Config(
                    Config.enable_iceberg_commit_queue,
                    Config.iceberg_commit_queue_timeout_seconds,
                    Config.iceberg_commit_queue_max_size
            );
            return queueConfig;
        });
        LOG.info("IcebergCommitQueueManager initialized for catalog {}: enabled={}, timeoutSeconds={}, maxSize={}",
                catalogName, Config.enable_iceberg_commit_queue,
                Config.iceberg_commit_queue_timeout_seconds, Config.iceberg_commit_queue_max_size);

        if (!isResourceMappingCatalog(this.catalogName)) {
            registerProcedures();
        }
    }

    private IcebergCatalog buildIcebergNativeCatalog() {
        IcebergCatalogType nativeCatalogType = icebergCatalogProperties.getCatalogType();
        Configuration conf = hdfsEnvironment.getConfiguration();

        if (Config.enable_iceberg_custom_worker_thread) {
            LOG.info("Default iceberg worker thread number changed " + Config.iceberg_worker_num_threads);
            Properties props = System.getProperties();
            props.setProperty(ThreadPools.WORKER_THREAD_POOL_SIZE_PROP,
                    String.valueOf(Config.iceberg_worker_num_threads));
        }

        Map<String, String> catalogProperties = withIcebergSseProperties(properties);

        switch (nativeCatalogType) {
            case HIVE_CATALOG:
                return new IcebergHiveCatalog(catalogName, conf, catalogProperties);
            case GLUE_CATALOG:
                return new IcebergGlueCatalog(catalogName, conf, catalogProperties);
            case REST_CATALOG:
                return new IcebergRESTCatalog(catalogName, conf, catalogProperties);
            case HADOOP_CATALOG:
                return new IcebergHadoopCatalog(catalogName, conf, catalogProperties);
            case JDBC_CATALOG:
                return new IcebergJdbcCatalog(catalogName, conf, catalogProperties);
            default:
                throw new StarRocksConnectorException("Property %s is missing or not supported now.",
                        ICEBERG_CATALOG_TYPE);
        }
    }

    // Translate the StarRocks aws.s3.sse.* properties into the Iceberg S3FileIO s3.sse.* properties so that
    // S3FileIO adds the SSE-C headers to every metadata GetObject/HeadObject. The customer key is only added
    // to the map handed to the native catalog, never to the catalog's stored properties, so it is not exposed
    // by SHOW CREATE CATALOG. Returns the original map untouched when SSE-C is not requested.
    @VisibleForTesting
    static Map<String, String> withIcebergSseProperties(Map<String, String> catalogProperties) {
        if (!AwsSseCUtil.isSseCEnabled(catalogProperties)) {
            return catalogProperties;
        }
        String keyMd5 = catalogProperties.getOrDefault(CloudConfigurationConstants.AWS_S3_SSE_KEY_MD5,
                AwsSseCUtil.validateAndGetKeyMd5(catalogProperties));
        Map<String, String> augmented = new HashMap<>(catalogProperties);
        augmented.put(S3FileIOProperties.SSE_TYPE, S3FileIOProperties.SSE_TYPE_CUSTOM);
        augmented.put(S3FileIOProperties.SSE_KEY, catalogProperties.get(CloudConfigurationConstants.AWS_S3_SSE_KEY));
        augmented.put(S3FileIOProperties.SSE_MD5, keyMd5);
        return augmented;
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new IcebergMetadata(catalogName, hdfsEnvironment, getNativeCatalog(),
                buildIcebergJobPlanningExecutor(), icebergCatalogProperties,
                connectorProperties, procedureRegistry, commitQueueManager);
    }

    // In order to be compatible with the catalog created with the wrong configuration,
    // icebergNativeCatalog is lazy, mainly to prevent fe restart failure.
    public IcebergCatalog getNativeCatalog() {
        if (icebergNativeCatalog == null) {
            IcebergCatalog nativeCatalog = buildIcebergNativeCatalog();

            if (icebergCatalogProperties.isEnableIcebergMetadataCache() && !isResourceMappingCatalog(catalogName)) {
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

    private ExecutorService buildBackgroundJobPlanningExecutor() {
        return newWorkerPool(catalogName + "-background-iceberg-worker-pool",
                icebergCatalogProperties.getBackgroundIcebergJobPlanningThreadNum());
    }

    private void registerProcedures() {
        this.procedureRegistry.register(new RegisterTableProcedure(catalogName, getNativeCatalog()));
    }

    @Override
    public void shutdown() {
        GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor()
                .unRegisterCachingIcebergCatalog(catalogName);
        if (icebergJobPlanningExecutor != null) {
            icebergJobPlanningExecutor.shutdown();
        }
        if (commitQueueManager != null) {
            commitQueueManager.shutdownAll();
            LOG.info("IcebergCommitQueueManager shutdown for catalog {}", catalogName);
        }
    }

    @Override
    public boolean supportMemoryTrack() {
        return icebergCatalogProperties.isEnableIcebergMetadataCache() && icebergNativeCatalog != null;
    }

    @Override
    public Map<String, Long> estimateCount() {
        return icebergNativeCatalog.estimateCount();
    }

    @Override
    public long estimateSize() {
        return icebergNativeCatalog.estimateSize();
    }
}
