// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.starrocks.server.CatalogMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MockedMetadataMgr extends MetadataMgr {
    private final Map<String, ConnectorMetadata> metadatas = new HashMap<>();
    private final LocalMetastore localMetastore;

    public MockedMetadataMgr(LocalMetastore localMetastore, ConnectorMgr connectorMgr) {
        super(localMetastore, connectorMgr, new ConnectorTblMetaInfoMgr());
        this.localMetastore = localMetastore;
    }

    public void registerMockedMetadata(String catalogName, ConnectorMetadata metadata) {
        metadatas.put(catalogName, metadata);
    }

    @Override
    public Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            return Optional.of(localMetastore);
        } else {
            return Optional.ofNullable(metadatas.get(catalogName));
        }
    }
}
