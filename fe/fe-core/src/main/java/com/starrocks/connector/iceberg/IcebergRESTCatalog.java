// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.connector.iceberg;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.HdfsEnvironment;
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

    public static synchronized IcebergRESTCatalog getInstance(Map<String, String> properties, HdfsEnvironment hdfsEnvironment) {
        String uri = properties.get(CatalogProperties.URI);
        return REST_URI_TO_CATALOG.computeIfAbsent(
            uri,
            key ->
                (IcebergRESTCatalog) CatalogLoader.rest(
                    String.format("rest-%s", uri), hdfsEnvironment.getConfiguration(), properties
                ).loadCatalog()
        );
    }

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return IcebergCatalogType.REST_CATALOG;
    }

    @Override
    public Table loadTable(IcebergTable table) throws StarRocksIcebergException {
        return super.loadTable(TableIdentifier.of(table.getRemoteDbName(), table.getRemoteTableName()));
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
