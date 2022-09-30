// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.delta;

import com.google.common.base.Preconditions;
import com.starrocks.common.util.Util;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.hive.HiveConnectorInternalMgr;
import com.starrocks.external.hive.CachingHiveMetastore;
import com.starrocks.external.hive.HiveMetastoreOperations;
import com.starrocks.external.hive.IHiveMetastore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static com.starrocks.external.hive.CachingHiveMetastore.createQueryLevelInstance;

public class DeltaLakeConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeConnector.class);

    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private final Map<String, String> properties;
    private final String catalogName;
    private ConnectorMetadata metadata;
    private final HiveConnectorInternalMgr internalMgr;

    public DeltaLakeConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        this.internalMgr = new HiveConnectorInternalMgr(catalogName, properties);
        validate();
        onCreate();
    }

    public void validate() {
        String hiveMetastoreUris = Preconditions.checkNotNull(properties.get(HIVE_METASTORE_URIS),
                "%s must be set in properties when creating hive catalog", HIVE_METASTORE_URIS);
        Util.validateMetastoreUris(hiveMetastoreUris);
    }


    @Override
    public ConnectorMetadata getMetadata() {
        if (metadata == null) {
            try {
                IHiveMetastore metastore = internalMgr.createHiveMetastore();
                HiveMetastoreOperations hiveMetastoreOperations = new HiveMetastoreOperations(
                        createQueryLevelInstance(metastore, internalMgr.getHiveMetastoreConf().getPerQueryCacheMaxNum()),
                        metastore instanceof CachingHiveMetastore);
                metadata = new DeltaLakeMetadata(properties.get(HIVE_METASTORE_URIS), catalogName,
                        hiveMetastoreOperations);
            } catch (Exception e) {
                LOG.error("Failed to create delta lake metadata on [catalog : {}]", catalogName, e);
                throw e;
            }
        }
        return metadata;
    }

    public void onCreate() {
    }

    @Override
    public void shutdown() {
        internalMgr.shutdown();
    }
}
