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


package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.common.util.Util;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileIO;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HiveConnector implements Connector {
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final String HIVE_METASTORE_TYPE = "hive.metastore.type";
    public static final List<String> SUPPORTED_METASTORE_TYPE = Lists.newArrayList("hive", "glue", "dlf");
    private final Map<String, String> properties;
    private final CloudConfiguration cloudConfiguration;
    private final String catalogName;
    private final HiveConnectorInternalMgr internalMgr;
    private final HiveMetadataFactory metadataFactory;
    private final InfoSchemaDb infoSchemaDb;

    public HiveConnector(ConnectorContext context) {
        this.properties = context.getProperties();
        this.catalogName = context.getCatalogName();
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
        this.internalMgr = new HiveConnectorInternalMgr(catalogName, properties, hdfsEnvironment);
        this.metadataFactory = createMetadataFactory(hdfsEnvironment);
        this.infoSchemaDb = new InfoSchemaDb(catalogName);
        validate();
        onCreate();
    }

    public void validate() {
        String hiveMetastoreType = properties.getOrDefault(HIVE_METASTORE_TYPE, "hive").toLowerCase();
        if (!SUPPORTED_METASTORE_TYPE.contains(hiveMetastoreType)) {
            throw new SemanticException("hive metastore type [%s] is not supported", hiveMetastoreType);
        }

        if (hiveMetastoreType.equals("hive")) {
            String hiveMetastoreUris = Preconditions.checkNotNull(properties.get(HIVE_METASTORE_URIS),
                    "%s must be set in properties when creating hive catalog", HIVE_METASTORE_URIS);
            Util.validateMetastoreUris(hiveMetastoreUris);
        }
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return metadataFactory.create();
    }

    private HiveMetadataFactory createMetadataFactory(HdfsEnvironment hdfsEnvironment) {
        IHiveMetastore metastore = internalMgr.createHiveMetastore();
        RemoteFileIO remoteFileIO = internalMgr.createRemoteFileIO();

        return new HiveMetadataFactory(
                catalogName,
                metastore,
                remoteFileIO,
                internalMgr.getHiveMetastoreConf(),
                internalMgr.getRemoteFileConf(),
                internalMgr.getPullRemoteFileExecutor(),
                internalMgr.isSearchRecursive(),
                internalMgr.enableHmsEventsIncrementalSync(),
                hdfsEnvironment.getConfiguration()
        );
    }

    public void onCreate() {
        Optional<CacheUpdateProcessor> updateProcessor = metadataFactory.getCacheUpdateProcessor();
        if (internalMgr.enableHmsEventsIncrementalSync()) {
            updateProcessor.ifPresent(processor -> GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor()
                    .registerCacheUpdateProcessor(catalogName, updateProcessor.get()));
        } else {
            if (!CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog(catalogName) &&
                    internalMgr.isEnableBackgroundRefreshHiveMetadata()) {
                updateProcessor
                        .ifPresent(processor -> GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor()
                                .registerCacheUpdateProcessor(catalogName, updateProcessor.get()));
            }
        }
    }

    @Override
    public void shutdown() {
        internalMgr.shutdown();
        metadataFactory.getCacheUpdateProcessor().ifPresent(CacheUpdateProcessor::invalidateAll);
        GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor().unRegisterCacheUpdateProcessor(catalogName);
        GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor().unRegisterCacheUpdateProcessor(catalogName);
    }

    public CloudConfiguration getCloudConfiguration() {
        return cloudConfiguration;
    }
}
