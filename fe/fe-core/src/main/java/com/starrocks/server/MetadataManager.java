// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.server;

import com.starrocks.connector.ConnectorMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.GuardedBy;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataManager {
    private static final Logger LOG = LogManager.getLogger(MetadataManager.class);
    @GuardedBy("this")
    private final ConcurrentHashMap<String, ConnectorMetadata> connectorsMetadata = new ConcurrentHashMap<>();

    public void addMetadata(String catalogName, ConnectorMetadata metadata) {
        connectorsMetadata.put(catalogName, metadata);
    }

    public void removeMetadata(String catalogName) {
        connectorsMetadata.remove(catalogName);
    }

    // get metadata by catalog name
    private Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
        // TODO: return local metastore for default internal catalog
        return Optional.of(connectorsMetadata.get(catalogName));
    }
}
