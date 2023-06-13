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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.types.DataField;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class PaimonMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(PaimonMetadata.class);
    private final Catalog paimonCatalog;
    private final String catalogType;
    private final String metastoreUris;
    private final String warehousePath;
    private final String catalogName;
    public PaimonMetadata(String catalogName, Catalog paimonCatalog,
                          String catalogType, String metastoreUris, String warehousePath) {
        this.paimonCatalog = paimonCatalog;
        this.catalogType = catalogType;
        this.metastoreUris = metastoreUris;
        this.warehousePath = warehousePath;
        this.catalogName = catalogName;
    }

    @Override
    public List<String> listDbNames() {
        return paimonCatalog.listDatabases();
    }

    @Override
    public List<String> listTableNames(String dbName) {
        try {
            return paimonCatalog.listTables(dbName);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new StarRocksConnectorException("Database %s not exists", dbName);
        }
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        Identifier identifier = new Identifier(databaseName, tableName);
        org.apache.paimon.table.Table paimonTable;
        try {
            paimonTable = this.paimonCatalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            throw new StarRocksConnectorException(String.format("Paimon table %s.%s does not exist.", databaseName, tableName));
        }
        return paimonTable.partitionKeys();
    }

    @Override
    public Database getDb(String dbName) {
        return new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), dbName);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        Identifier identifier = new Identifier(dbName, tblName);
        org.apache.paimon.table.Table paimonTable;
        try {
            paimonTable = this.paimonCatalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            throw new StarRocksConnectorException("Paimon table {}.{} does not exist.", dbName, tblName);
        }
        List<DataField> fields = paimonTable.rowType().getFields();
        ArrayList<Column> fullSchema = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            String fieldName = field.name();
            Type fieldType = ColumnTypeConverter.fromPaimonType(field.type());
            Column column = new Column(fieldName, fieldType, true);
            fullSchema.add(column);
        }
        return new PaimonTable(catalogName, dbName, tblName, fullSchema, catalogType, metastoreUris, warehousePath, paimonTable);
    }
}
