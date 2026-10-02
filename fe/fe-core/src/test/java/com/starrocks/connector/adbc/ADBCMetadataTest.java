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
import com.starrocks.catalog.Table;
import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ADBCMetadataTest {
    @Mocked
    private AdbcDatabase database;

    @Mocked
    private AdbcConnection connection;

    @Mocked
    private ArrowReader reader;

    @Mocked
    private VectorSchemaRoot root;

    @Mocked
    private ListVector schemas;

    private ADBCMetadata metadata() {
        return new ADBCMetadata(Map.of("driver", "adbc_driver_flightsql"), "adbc_catalog", database);
    }

    @Test
    public void testListDatabasesReadsAllBatchesAndClosesResources() throws Exception {
        List<?> first = Arrays.asList(Map.of("db_schema_name", "analytics"),
                Map.of("db_schema_name", ""), Collections.singletonMap("db_schema_name", null), "unexpected");
        List<?> second = List.of(Map.of("db_schema_name", "analytics"), Map.of("db_schema_name", "sales"));
        new Expectations() {{
                database.connect();
                result = connection;
                connection.getObjects(AdbcConnection.GetObjectsDepth.DB_SCHEMAS, null, null, null, null, null);
                result = reader;
                reader.loadNextBatch();
                returns(true, true, false);
                reader.getVectorSchemaRoot();
                result = root;
                root.getVector("catalog_db_schemas");
                result = schemas;
                root.getRowCount();
                result = 3;
                schemas.isNull(2);
                result = true;
                schemas.getObject(0);
                returns(first, second);
                schemas.getObject(1);
                result = null;
            }};

        assertEquals(List.of("analytics", "sales"), metadata().listDbNames(null));
        new Verifications() {{
                reader.close();
                times = 1;
                connection.close();
                times = 1;
            }};
    }

    @Test
    public void testGetDbMatchesOnlyReturnedSchemaNames() throws Exception {
        new Expectations() {{
                database.connect();
                result = connection;
                connection.getObjects(AdbcConnection.GetObjectsDepth.DB_SCHEMAS, null, null, null, null, null);
                result = reader;
                reader.loadNextBatch();
                returns(true, false, true, false);
                reader.getVectorSchemaRoot();
                result = root;
                root.getVector("catalog_db_schemas");
                result = schemas;
                root.getRowCount();
                result = 1;
                schemas.getObject(0);
                result = List.of(Map.of("db_schema_name", "analytics"));
            }};

        ADBCMetadata metadata = metadata();
        assertEquals(Table.TableType.ADBC, metadata.getTableType());
        assertNotNull(metadata.getDb(null, "analytics"));
        assertNull(metadata.getDb(null, "missing"));
    }

    @Test
    public void testListDatabasesSkipsMissingSchemaVector() throws Exception {
        new Expectations() {{
                database.connect();
                result = connection;
                connection.getObjects(AdbcConnection.GetObjectsDepth.DB_SCHEMAS, null, null, null, null, null);
                result = reader;
                reader.loadNextBatch();
                returns(true, false);
                reader.getVectorSchemaRoot();
                result = root;
                root.getVector("catalog_db_schemas");
                result = null;
            }};

        assertEquals(List.of(), metadata().listDbNames(null));
    }

    @Test
    public void testListTablesFiltersSchemasAndMalformedEntries() throws Exception {
        List<?> values = List.of("unexpected",
                Map.of("db_schema_name", "analytics", "db_schema_tables", List.of(Map.of("table_name", "events"))),
                Map.of("db_schema_name", "sales", "db_schema_tables", "not a list"),
                Map.of("db_schema_name", "sales", "db_schema_tables", Arrays.asList(
                        Map.of("table_name", "orders"), "unexpected", Collections.singletonMap("table_name", null),
                        Map.of("table_name", "orders"))));
        new Expectations() {{
                database.connect();
                result = connection;
                connection.getObjects(AdbcConnection.GetObjectsDepth.TABLES, null, "sales", null, null, null);
                result = reader;
                reader.loadNextBatch();
                returns(true, true, false);
                reader.getVectorSchemaRoot();
                result = root;
                root.getVector("catalog_db_schemas");
                returns(null, schemas);
                root.getRowCount();
                result = 3;
                schemas.isNull(2);
                result = true;
                schemas.getObject(0);
                result = values;
                schemas.getObject(1);
                result = null;
            }};

        assertEquals(List.of("orders"), metadata().listTableNames(null, "sales"));
        new Verifications() {{
                reader.close();
                times = 1;
                connection.close();
                times = 1;
            }};
    }

    @Test
    public void testListingFailurePreservesContextAndClosesResources() throws Exception {
        IllegalStateException failure = new IllegalStateException("metadata stream failed");
        new Expectations() {{
                database.connect();
                result = connection;
                connection.getObjects(AdbcConnection.GetObjectsDepth.DB_SCHEMAS, null, null, null, null, null);
                result = reader;
                connection.getObjects(AdbcConnection.GetObjectsDepth.TABLES, null, "sales", null, null, null);
                result = reader;
                reader.loadNextBatch();
                result = failure;
            }};

        ADBCMetadata metadata = metadata();
        StarRocksConnectorException databases = assertThrows(StarRocksConnectorException.class,
                () -> metadata.listDbNames(null));
        StarRocksConnectorException tables = assertThrows(StarRocksConnectorException.class,
                () -> metadata.listTableNames(null, "sales"));
        assertSame(failure, databases.getCause());
        assertSame(failure, tables.getCause());
        assertTrue(databases.getMessage().contains("adbc_catalog"));
        assertTrue(databases.getMessage().contains("list databases"));
        assertTrue(tables.getMessage().contains("list tables in database 'sales'"));
        new Verifications() {{
                reader.close();
                times = 2;
                connection.close();
                times = 2;
            }};
    }

    @Test
    public void testMissingAndFailedTableLookupsReturnNullAndCloseConnection() throws Exception {
        new Expectations() {{
                database.connect();
                result = connection;
                connection.getTableSchema(null, "sales", "missing");
                result = null;
                connection.getTableSchema(null, "sales", "empty");
                result = new Schema(List.of());
                connection.getTableSchema(null, "sales", "failed");
                result = new IllegalStateException("unavailable");
            }};

        ADBCMetadata metadata = metadata();
        assertNull(metadata.getTable(null, "sales", "missing"));
        assertNull(metadata.getTable(null, "sales", "empty"));
        assertNull(metadata.getTable(null, "sales", "failed"));
        new Verifications() {{
                connection.close();
                times = 3;
            }};
    }

    @Test
    public void testShutdownClosesDatabaseAndToleratesDriverFailure() throws Exception {
        new Expectations() {{
                database.close();
                result = new IllegalStateException("driver close failed");
            }};

        assertDoesNotThrow(() -> metadata().shutdown());
        new Verifications() {{
                database.close();
                times = 1;
            }};
    }

    @Test
    public void testDbSchemaMapping() {
        Set<String> names = new LinkedHashSet<>();
        ADBCMetadata.collectDbSchemaNames(List.of(
                Map.of("db_schema_name", "analytics"),
                Map.of("db_schema_name", "sales"),
                Map.of("db_schema_name", "analytics")), names);

        assertEquals(List.of("analytics", "sales"), new ArrayList<>(names));
    }

    @Test
    public void testTableMappingUsesRequestedDbSchema() {
        Set<String> names = new LinkedHashSet<>();
        ADBCMetadata.collectTableNames(List.of(
                Map.of("db_schema_name", "analytics", "db_schema_tables",
                        List.of(Map.of("table_name", "events"))),
                Map.of("db_schema_name", "sales", "db_schema_tables",
                        List.of(Map.of("table_name", "orders")))), "sales", names);

        assertEquals(Set.of("orders"), names);
    }

    @Test
    public void testGetTableConvertsArrowSchema() throws Exception {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), List.of()),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), List.of())));
        new Expectations() {{
                database.connect();
                result = connection;
                connection.getTableSchema(null, "analytics", "events");
                result = schema;
            }};

        Map<String, String> properties = new HashMap<>();
        properties.put("driver", "adbc_driver_flightsql");
        properties.put("uri", "grpc+tcp://localhost:32010");
        ADBCMetadata metadata = new ADBCMetadata(properties, "adbc_catalog", database);

        Table table = metadata.getTable(null, "analytics", "events");

        assertNotNull(table);
        assertInstanceOf(ADBCTable.class, table);
        assertEquals("analytics", ((ADBCTable) table).getDbName());
        assertEquals(List.of("id", "name"), table.getFullSchema().stream()
                .map(column -> column.getName()).toList());
        assertEquals(table.getId(), metadata.getTable(null, "analytics", "events").getId());
        metadata.clear();
        assertNotEquals(table.getId(), metadata.getTable(null, "analytics", "events").getId());
        new Verifications() {{
                connection.close();
                times = 3;
            }};
    }
}
