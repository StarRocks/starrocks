// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.server;

import com.starrocks.connector.ConnectorMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.GuardedBy;

public class MetadataMgr {
    private static final Logger LOG = LogManager.getLogger(MetadataMgr.class);

    @GuardedBy("this")
    private final ConcurrentHashMap<String, ConnectorMetadata> connectorMetadatas = new ConcurrentHashMap<>();

    public void addMetadata(String catalogName, ConnectorMetadata metadata) {
        connectorMetadatas.put(catalogName, metadata);
    }

    public void removeMetadata(String catalogName) {
        connectorMetadatas.remove(catalogName);
    }

    // get metadata by catalog name
    private Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
        // TODO: return local metastore for default internal catalog
        return Optional.of(connectorMetadatas.get(catalogName));
    }
}
