package com.starrocks.server;

import com.google.common.collect.Maps;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.common.DdlException;
import com.starrocks.spi.Metadata;
import com.starrocks.spi.TableIdentifier;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataManager {
    private final LocalMetastore metastore;
    private ConcurrentHashMap<String, Metadata> connectorMetadatas = new ConcurrentHashMap<>();

    public MetadataManager(LocalMetastore metastore) {
        this.metastore = metastore;
    }

    public void addMetadata() {
    }

    public void removeMetadata() {
    }

    // get metadata by catalog name
    private Optional<Metadata> getOptionalMetadata(String catalogName) {
        return Optional.empty();
    }

    public List<String> listDatabaseNames(TableIdentifier tableIdentifier) throws DdlException {
        Optional<Metadata> metadata = getOptionalMetadata(tableIdentifier.getCatalogName());
        if (!metadata.isPresent()) {
            // throw exception
            return Collections.emptyList();
        }

        return metadata.get().listDatabaseNames();
    }

    public void createTable(CreateTableStmt createTableStmt) throws DdlException {
        // must use localMetastore at present
        // check if the catalog is default in createTableStmt otherwise throw exception.
        metastore.createTable(createTableStmt);
    }

}
