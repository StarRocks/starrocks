// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hudi;

import com.google.common.base.Preconditions;
import com.starrocks.common.util.Util;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;

import java.util.Map;

public class HudiConnector implements Connector {
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private final Map<String, String> properties;
    private String resourceName;
    private ConnectorMetadata metadata;

    public HudiConnector(ConnectorContext context) {
        this.properties = context.getProperties();
        validate();
    }

    public void validate() {
        this.resourceName = Preconditions.checkNotNull(properties.get(HIVE_METASTORE_URIS),
                "%s must be set in properties when creating hive catalog", HIVE_METASTORE_URIS);
        Util.validateMetastoreUris(resourceName);
    }

    @Override
    public ConnectorMetadata getMetadata() {
        if (metadata == null) {
            metadata = new HudiMetadata(resourceName);
        }
        return metadata;
    }

}