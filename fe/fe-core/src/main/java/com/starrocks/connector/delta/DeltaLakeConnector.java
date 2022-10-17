// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.delta;

import com.google.common.base.Preconditions;
import com.starrocks.common.util.Util;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.external.hive.IHiveMetastore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class DeltaLakeConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeConnector.class);

    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private final Map<String, String> properties;
    private final String catalogName;
    private final DeltaLakeInternalMgr internalMgr;
    private final DeltaLakeMetadataFactory metadataFactory;

    public DeltaLakeConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        this.internalMgr = new DeltaLakeInternalMgr(catalogName, properties);
        this.metadataFactory = createMetadataFactory();
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
        return metadataFactory.create();
    }

    private DeltaLakeMetadataFactory createMetadataFactory() {
        IHiveMetastore metastore = internalMgr.createHiveMetastore();

        return new DeltaLakeMetadataFactory(
                catalogName,
                metastore,
                internalMgr.getHiveMetastoreConf(),
                properties.get(HIVE_METASTORE_URIS)
        );
    }

    public void onCreate() {
    }

    @Override
    public void shutdown() {
        internalMgr.shutdown();
    }
}
