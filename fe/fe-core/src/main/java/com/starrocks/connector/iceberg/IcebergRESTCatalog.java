// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.starrocks.connector.iceberg.IcebergUtil.convertToSRDatabase;

public class IcebergRESTCatalog extends RESTCatalog implements IcebergCatalog {

    private static final ConcurrentHashMap<String, IcebergRESTCatalog> REST_URI_TO_CATALOG =
            new ConcurrentHashMap<>();

    public static synchronized IcebergRESTCatalog getInstance(Map<String, String> properties) {
        String uri = properties.get(CatalogProperties.URI);
        return REST_URI_TO_CATALOG.computeIfAbsent(
            uri,
            key ->
                (IcebergRESTCatalog) CatalogLoader.rest(
                    String.format("rest-%s", uri), new Configuration(), properties
                ).loadCatalog()
        );
    }

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return IcebergCatalogType.REST_CATALOG;
    }

    @Override
    public Table loadTable(IcebergTable table) throws StarRocksIcebergException {
        return super.loadTable(TableIdentifier.of(table.getDb(), table.getTable()));
    }

    @Override
    public Table loadTable(TableIdentifier tableId, String tableLocation,
            Map<String, String> properties) throws StarRocksIcebergException {
        return super.loadTable(tableId);
    }

    @Override
    public List<String> listAllDatabases() {
        return super.listNamespaces().stream().map(Namespace::toString)
            .collect(Collectors.toList());
    }

    @Override
    public Database getDB(String dbName) throws TException {
        if (!super.namespaceExists(Namespace.of(dbName))) {
            throw new TException("Iceberg db " + dbName + " doesn't exist");
        }
        return convertToSRDatabase(dbName);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name())
            .add("uri", this.properties().get(CatalogProperties.URI))
            .toString();
    }
}
