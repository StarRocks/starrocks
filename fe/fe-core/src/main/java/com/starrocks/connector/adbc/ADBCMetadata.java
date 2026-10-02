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

package com.starrocks.connector.adbc;

import com.starrocks.catalog.ADBCTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metadata implementation for the first ADBC catalog capability profile.
 *
 * <p>StarRocks databases map deterministically to ADBC {@code db_schema}
 * values. ADBC catalogs are intentionally not exposed or guessed in this MVP.
 * Drivers that need a different hierarchy mapping can add a metadata profile
 * without changing the catalog or scan contracts.
 */
public class ADBCMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(ADBCMetadata.class);

    private final String catalogName;
    private final Map<String, String> properties;
    private final AdbcDatabase database;
    private final ADBCSchemaResolver schemaResolver = new ADBCSchemaResolver();
    private final ConcurrentHashMap<ADBCTableName, Long> tableIds = new ConcurrentHashMap<>();

    public ADBCMetadata(Map<String, String> properties, String catalogName, AdbcDatabase database) {
        this.properties = properties;
        this.catalogName = catalogName;
        this.database = database;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.ADBC;
    }

    @Override
    public Database getDb(ConnectContext context, String name) {
        return listDbNames(context).contains(name) ? new Database(0, name) : null;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        Set<String> names = new LinkedHashSet<>();
        try (AdbcConnection connection = database.connect();
                ArrowReader reader = connection.getObjects(
                        AdbcConnection.GetObjectsDepth.DB_SCHEMAS,
                        null, null, null, null, null)) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                ListVector schemas = (ListVector) root.getVector("catalog_db_schemas");
                if (schemas == null) {
                    continue;
                }
                for (int row = 0; row < root.getRowCount(); row++) {
                    if (schemas.isNull(row)) {
                        continue;
                    }
                    List<?> values = schemas.getObject(row);
                    if (values == null) {
                        continue;
                    }
                    collectDbSchemaNames(values, names);
                }
            }
            return new ArrayList<>(names);
        } catch (Exception e) {
            throw metadataError("list databases", e);
        }
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        Set<String> names = new LinkedHashSet<>();
        try (AdbcConnection connection = database.connect();
                ArrowReader reader = connection.getObjects(
                        AdbcConnection.GetObjectsDepth.TABLES,
                        null, dbName, null, null, null)) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                ListVector schemas = (ListVector) root.getVector("catalog_db_schemas");
                if (schemas == null) {
                    continue;
                }
                for (int row = 0; row < root.getRowCount(); row++) {
                    if (schemas.isNull(row)) {
                        continue;
                    }
                    collectTableNames(schemas.getObject(row), dbName, names);
                }
            }
            return new ArrayList<>(names);
        } catch (Exception e) {
            throw metadataError("list tables in database '" + dbName + "'", e);
        }
    }

    static void collectDbSchemaNames(List<?> schemas, Set<String> names) {
        if (schemas == null) {
            return;
        }
        for (Object value : schemas) {
            if (value instanceof Map) {
                Object name = ((Map<?, ?>) value).get("db_schema_name");
                if (name != null && !name.toString().isEmpty()) {
                    names.add(name.toString());
                }
            }
        }
    }

    static void collectTableNames(List<?> schemas, String dbName, Set<String> names) {
        if (schemas == null) {
            return;
        }
        for (Object value : schemas) {
            if (!(value instanceof Map)) {
                continue;
            }
            Map<?, ?> schema = (Map<?, ?>) value;
            if (!dbName.equals(String.valueOf(schema.get("db_schema_name")))) {
                continue;
            }
            Object tables = schema.get("db_schema_tables");
            if (!(tables instanceof List)) {
                continue;
            }
            for (Object tableValue : (List<?>) tables) {
                if (tableValue instanceof Map) {
                    Object name = ((Map<?, ?>) tableValue).get("table_name");
                    if (name != null) {
                        names.add(name.toString());
                    }
                }
            }
        }
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tableName) {
        try (AdbcConnection connection = database.connect()) {
            Schema arrowSchema = connection.getTableSchema(null, dbName, tableName);
            if (arrowSchema == null) {
                return null;
            }
            List<Column> columns = schemaResolver.convertToSRTable(arrowSchema);
            if (columns.isEmpty()) {
                return null;
            }
            ADBCTableName key = ADBCTableName.of(catalogName, dbName, tableName);
            long tableId = tableIds.computeIfAbsent(
                    key, ignored -> ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asLong());
            return new ADBCTable(tableId, tableName, columns, dbName, catalogName, properties);
        } catch (Exception e) {
            LOG.warn("Failed to load ADBC table '{}.{}.{}'", catalogName, dbName, tableName, e);
            return null;
        }
    }

    private StarRocksConnectorException metadataError(String operation, Exception cause) {
        return new StarRocksConnectorException(
                "ADBC catalog '" + catalogName + "': failed to " + operation
                        + ". Detail: " + cause.getMessage(), cause);
    }

    @Override
    public void refreshTable(String dbName, Table table,
                             List<String> partitionNames, boolean onlyCachedPartitions) {
        // Metadata is fetched directly from the driver.
    }

    @Override
    public void clear() {
        tableIds.clear();
    }

    @Override
    public void shutdown() {
        try {
            database.close();
        } catch (Exception e) {
            LOG.warn("Failed to close ADBC catalog '{}'", catalogName, e);
        }
    }
}
