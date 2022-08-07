// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector.iceberg;

import com.google.common.base.Preconditions;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.Util;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static com.starrocks.catalog.IcebergTable.ICEBERG_METASTORE_URIS;

public class IcebergConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(IcebergConnector.class);

    private final Map<String, String> properties;
    private final String catalogName;
    private String metastoreURI;
    private ConnectorMetadata metadata;

    public IcebergConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        validate();
    }

    public void validate() {
        this.metastoreURI = Preconditions.checkNotNull(properties.get(ICEBERG_METASTORE_URIS),
                "%s must be set in properties when creating iceberg catalog", ICEBERG_METASTORE_URIS);
        Util.validateMetastoreUris(metastoreURI);
    }

    @Override
    public ConnectorMetadata getMetadata() {
        if (metadata == null) {
            try {
                metadata = new IcebergMetadata(metastoreURI);
            } catch (Exception e) {
                LOG.error("Failed to create iceberg metadata on [catalog : {}]", catalogName, e);
                throw e;
            }
        }
        return metadata;
    }
}
