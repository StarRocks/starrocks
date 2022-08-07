// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector.hive;

import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.external.RemoteFileIO;
import com.starrocks.external.hive.HiveConnectorInternalMgr;
import com.starrocks.external.hive.HiveMetadataFactory;
import com.starrocks.external.hive.HiveStatisticsProvider;
import com.starrocks.external.hive.IHiveMetastore;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class HiveConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(HiveConnector.class);
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private final Map<String, String> properties;
    private final String catalogName;
    private HiveMetadataFactory metadataFactory;
    private HiveConnectorInternalMgr internalMgr;


    public HiveConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
//        validate();
//        onCreate();
        this.internalMgr = new HiveConnectorInternalMgr(catalogName, properties);
        initMetadataFactory();
    }
//
//
//    public void validate() {
//        this.resourceName = Preconditions.checkNotNull(properties.get(HIVE_METASTORE_URIS),
//                "%s must be set in properties when creating hive catalog", HIVE_METASTORE_URIS);
//        Util.validateMetastoreUris(resourceName);
//    }

    @Override
    public ConnectorMetadata getMetadata() {
        return metadataFactory.create();
//        if (metadata == null) {
//            try {
//                metadata = new HiveMetadata(resourceName);
//            } catch (Exception e) {
//                LOG.error("Failed to create hive metadata on [catalog : {}]", catalogName, e);
//                throw e;
//            }
//        }
//        return metadata;
    }


    public void onCreate() {
//        if (Config.enable_hms_events_incremental_sync) {
//            GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor()
//                    .registerExternalCatalogResource(resourceName);
//        }
    }

    @Override
    public void shutdown() {
//        if (Config.enable_hms_events_incremental_sync) {
//            GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor()
//                    .unregisterExternalCatalogResource(resourceName);
//        }
    }

    private void initMetadataFactory() {
        IHiveMetastore metastore = internalMgr.createHiveMetastore();
        RemoteFileIO remoteFileIO = internalMgr.createRemoteFileIO();
        metadataFactory = new HiveMetadataFactory(metastore, remoteFileIO, 10000);
    }
}
