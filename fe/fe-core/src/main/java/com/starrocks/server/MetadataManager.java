package com.starrocks.server;

import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.spi.Metadata;
import com.starrocks.spi.TableIdentifier;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataManager {
    private final LocalMetastore metastore;
    private final ConcurrentHashMap<String, Metadata> connectorMetadatas = new ConcurrentHashMap<>();

    public MetadataManager(LocalMetastore metastore) {
        this.metastore = metastore;
    }

    public void addMetadata(String catalogName, Metadata metadata) {
        connectorMetadatas.put(catalogName, metadata);
    }

    public void removeMetadata() {
    }

    // get metadata by catalog name
    private Optional<Metadata> getOptionalMetadata(String catalogName) {
        if (catalogName.equals("default_catalog")) {
            return Optional.of(metastore);
        } else {
            return Optional.of(connectorMetadatas.get(catalogName));
        }
    }

    public List<String> listDatabaseNames(String catalogName) throws DdlException {
        Optional<Metadata> metadata = getOptionalMetadata(catalogName);
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

    public Table getTable(TableIdentifier identifier) {
        Optional<Metadata> metadata = getOptionalMetadata(identifier.getCatalogName());
        if (!metadata.isPresent()) {
            return null;
        }

        try {
            return metadata.get().getTable(identifier.getDatabase(), identifier.getTable());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public List<String> listTableNames(String catalogName, String dbName) {
        Optional<Metadata> metadata = getOptionalMetadata(catalogName);
        if (!metadata.isPresent()) {
            // throw exception
            return Collections.emptyList();
        }

        return metadata.get().listTableNames(dbName);
    }







}
