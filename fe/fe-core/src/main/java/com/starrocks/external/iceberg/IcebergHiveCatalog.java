// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.external.iceberg;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.IcebergTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IcebergHiveCatalog implements IcebergCatalog {
    private static ConcurrentHashMap<String, IcebergHiveCatalog> metastoreUriToCatalog = new ConcurrentHashMap<>();

    public static synchronized IcebergHiveCatalog getInstance(String uri) {
        if (!metastoreUriToCatalog.containsKey(uri)) {
            metastoreUriToCatalog.put(uri, new IcebergHiveCatalog(uri));
        }
        return metastoreUriToCatalog.get(uri);
    }

    private Catalog hiveCatalog;

    private IcebergHiveCatalog(String uri) {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.URI, uri);
        hiveCatalog = CatalogLoader.hive(String.format("hive-%s", uri),
                new Configuration(), properties).loadCatalog();
    }

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return IcebergCatalogType.HIVE_CATALOG;
    }

    @Override
    public Table loadTable(IcebergTable table) throws StarRocksIcebergException {
        TableIdentifier tableId = IcebergUtil.getIcebergTableIdentifier(table);
        return loadTable(tableId, null, null);
    }

    @Override
    public Table loadTable(TableIdentifier tableIdentifier) throws StarRocksIcebergException {
        return loadTable(tableIdentifier, null, null);
    }

    @Override
    public Table loadTable(TableIdentifier tableId, String tableLocation,
                           Map<String, String> properties) throws StarRocksIcebergException {
        Preconditions.checkState(tableId != null);
        try {
            return hiveCatalog.loadTable(tableId);
        } catch (Exception e) {
            throw new StarRocksIcebergException(String.format(
                    "Failed to load Iceberg table with id: %s", tableId), e);
        }
    }
}
