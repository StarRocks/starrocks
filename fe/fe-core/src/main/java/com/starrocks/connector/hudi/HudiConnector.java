// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hudi;

import com.google.common.base.Preconditions;
import com.starrocks.common.util.Util;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileIO;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.server.GlobalStateMgr;

import java.util.Map;

public class HudiConnector implements Connector {
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private final Map<String, String> properties;
    private final CloudConfiguration cloudConfiguration;
    private final String catalogName;
    private final HudiConnectorInternalMgr internalMgr;
    private final HudiMetadataFactory metadataFactory;

    public HudiConnector(ConnectorContext context) {
        this.properties = context.getProperties();
        this.cloudConfiguration = CloudConfigurationFactory.tryBuildForStorage(properties);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(null, cloudConfiguration);
        this.catalogName = context.getCatalogName();
        this.internalMgr = new HudiConnectorInternalMgr(catalogName, properties, hdfsEnvironment);
        this.metadataFactory = createMetadataFactory();
        validate();
        onCreate();
    }

    public void validate() {
        String hiveMetastoreUris = Preconditions.checkNotNull(properties.get(HIVE_METASTORE_URIS),
                "%s must be set in properties when creating hudi catalog", HIVE_METASTORE_URIS);
        Util.validateMetastoreUris(hiveMetastoreUris);
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return metadataFactory.create();
    }

    private HudiMetadataFactory createMetadataFactory() {
        IHiveMetastore metastore = internalMgr.createHiveMetastore();
        RemoteFileIO remoteFileIO = internalMgr.createRemoteFileIO();
        return new HudiMetadataFactory(
                catalogName,
                metastore,
                remoteFileIO,
                internalMgr.getHiveMetastoreConf(),
                internalMgr.getRemoteFileConf(),
                internalMgr.getPullRemoteFileExecutor(),
                internalMgr.isSearchRecursive()
        );
    }

    public void onCreate() {
    }

    public CloudConfiguration getCloudConfiguration() {
        return cloudConfiguration;
    }

    @Override
    public void shutdown() {
        internalMgr.shutdown();
        GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor().unRegisterCacheUpdateProcessor(catalogName);
    }
}