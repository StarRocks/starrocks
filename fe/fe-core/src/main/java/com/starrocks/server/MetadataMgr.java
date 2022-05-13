// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.server;

import com.google.common.base.Preconditions;
import com.starrocks.connector.ConnectorMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataMgr {
    private static final Logger LOG = LogManager.getLogger(MetadataMgr.class);

    private final ConcurrentHashMap<String, ConnectorMetadata> connectorMetadatas = new ConcurrentHashMap<>();

    public MetadataMgr() {
    }

    public void addMetadata(String catalogName, ConnectorMetadata metadata) {
        Preconditions.checkState(!connectorMetadatas.containsKey(catalogName),
                "ConnectorMetadata of catalog '%s' already exists", catalogName);
        connectorMetadatas.put(catalogName, metadata);
    }

    public void removeMetadata(String catalogName) {
        connectorMetadatas.remove(catalogName);
    }

    public boolean connectorMetadataExists(String catalogName) {
        return connectorMetadatas.containsKey(catalogName);
    }

    // get metadata by catalog name
    public Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
        // TODO: return local metastore for default internal catalog
        return Optional.of(connectorMetadatas.get(catalogName));
    }
}
