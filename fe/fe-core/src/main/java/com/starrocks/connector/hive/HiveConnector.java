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

import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileIO;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;

import java.util.Map;
import java.util.Optional;

public class HiveConnector implements Connector {
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final String HIVE_METASTORE_TYPE = "hive.metastore.type";
    public static final String HIVE_METASTORE_TIMEOUT = "hive.metastore.timeout";
    private final Map<String, String> properties;
    private final String catalogName;
    private final HiveConnectorInternalMgr internalMgr;
    private final HiveMetadataFactory metadataFactory;

    public HiveConnector(ConnectorContext context) {
        this.properties = context.getProperties();
        this.catalogName = context.getCatalogName();
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
        this.internalMgr = new HiveConnectorInternalMgr(catalogName, properties, hdfsEnvironment);
        this.metadataFactory = createMetadataFactory(hdfsEnvironment);
        onCreate();
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
                internalMgr.getupdateRemoteFilesExecutor(),
                internalMgr.getUpdateStatisticsExecutor(),
                internalMgr.getRefreshOthersFeExecutor(),
                internalMgr.isSearchRecursive(),
                internalMgr.enableHmsEventsIncrementalSync(),
                hdfsEnvironment,
                internalMgr.getMetastoreType()
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
}
