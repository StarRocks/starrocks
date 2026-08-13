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
import mockit.Expectations;
import mockit.Mocked;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ADBCMetadataTest {
    @Mocked
    private AdbcDatabase database;

    @Mocked
    private AdbcConnection connection;

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
    }
}
