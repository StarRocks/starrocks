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

import com.starrocks.common.Pair;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DeltaLakeConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeConnector.class);

    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private final Map<String, String> properties;
    private final CloudConfiguration cloudConfiguration;
    private final String catalogName;
    private final DeltaLakeInternalMgr internalMgr;
    private final DeltaLakeMetadataFactory metadataFactory;
    private IDeltaLakeMetastore metastore;

    public DeltaLakeConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
        this.internalMgr = new DeltaLakeInternalMgr(catalogName, properties, hdfsEnvironment);
        this.metadataFactory = createMetadataFactory();
        onCreate();
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return metadataFactory.create();
    }

    private DeltaLakeMetadataFactory createMetadataFactory() {
        metastore = internalMgr.createDeltaLakeMetastore();
        return new DeltaLakeMetadataFactory(
                catalogName,
                metastore,
                internalMgr.getHiveMetastoreConf(),
                properties,
                internalMgr.getHdfsEnvironment(),
                internalMgr.getMetastoreType()
        );
    }

    public CloudConfiguration getCloudConfiguration() {
        return this.cloudConfiguration;
    }

    @Override
    public void shutdown() {
        internalMgr.shutdown();
        metadataFactory.metastoreCacheInvalidateCache();
        GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor().unRegisterCacheUpdateProcessor(catalogName);
    }

    public void onCreate() {
        Optional<DeltaLakeCacheUpdateProcessor> updateProcessor = metadataFactory.getCacheUpdateProcessor();
        updateProcessor.ifPresent(processor -> GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor()
                        .registerCacheUpdateProcessor(catalogName, updateProcessor.get()));
    }

    @Override
    public boolean supportMemoryTrack() {
        return metastore != null;
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        return metastore.getSamples();
    }

    @Override
    public Map<String, Long> estimateCount() {
        return metastore.estimateCount();
    }
}
