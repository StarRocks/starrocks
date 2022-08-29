// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.starrocks.common.Config;
import com.starrocks.common.util.Util;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.server.GlobalStateMgr;

import java.util.Map;

public class HiveConnector implements Connector {
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private final Map<String, String> properties;
    private String resourceName;
    private ConnectorMetadata metadata;

    public HiveConnector(ConnectorContext context) {
        this.properties = context.getProperties();
        validate();
        onCreate();
    }

    public void validate() {
        this.resourceName = Preconditions.checkNotNull(properties.get(HIVE_METASTORE_URIS),
                "%s must be set in properties when creating hive catalog", HIVE_METASTORE_URIS);
        Util.validateMetastoreUris(resourceName);
    }

    @Override
    public ConnectorMetadata getMetadata() {
        if (metadata == null) {
            metadata = new HiveMetadata(resourceName);
        }
        return metadata;
    }


    public void onCreate() {
        if (Config.enable_hms_events_incremental_sync) {
            GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor()
                    .registerExternalCatalogResource(resourceName);
        }
    }

    @Override
    public void shutdown() {
        if (Config.enable_hms_events_incremental_sync) {
            boolean existOtherCatalogWithSameUrl = GlobalStateMgr.getCurrentState().getCatalogMgr()
                    .existSameUrlCatalog(resourceName);
            if (!existOtherCatalogWithSameUrl) {
                GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor()
                        .unregisterExternalCatalogResource(resourceName);
            }
        }
    }
}
