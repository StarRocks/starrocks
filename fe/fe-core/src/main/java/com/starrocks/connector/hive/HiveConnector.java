// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.Util;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class HiveConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(HiveConnector.class);

    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private final Map<String, String> properties;
    private final String catalogName;
    private String resourceName;
    private ConnectorMetadata metadata;

    public HiveConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
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
    public ConnectorMetadata getMetadata() throws DdlException {
        if (metadata == null) {
            try {
                metadata = new HiveMetadata(resourceName);
            } catch (Exception e) {
                LOG.error("Failed to create hive metadata on [catalog : {}]", catalogName, e);
                throw e;
            }
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
