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

package com.starrocks.connector.benchmark;

import com.starrocks.catalog.Column;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BenchmarkTableSchemasTest {
    private static final String CUSTOM_SCHEMA_JSON =
            "{\n" +
                    "  \"tables\": [\n" +
                    "    {\n" +
                    "      \"name\": \"t1\",\n" +
                    "      \"columns\": [\n" +
                    "        {\"name\": \"c_int32\", \"type\": \"int32\"},\n" +
                    "        {\"name\": \"c_int64\", \"type\": \"int64\"},\n" +
                    "        {\"name\": \"c_string\", \"type\": \"string\"},\n" +
                    "        {\"name\": \"c_bool\", \"type\": \"bool\"},\n" +
                    "        {\"name\": \"c_float\", \"type\": \"float\"},\n" +
                    "        {\"name\": \"c_date32\", \"type\": \"date32\"},\n" +
                    "        {\"name\": \"c_date32_day\", \"type\": \"date32[day]\"},\n" +
                    "        {\"name\": \"c_decimal\", \"type\": \"decimal(10, 2)\"}\n" +
                    "      ]\n" +
                    "    },\n" +
                    "    {\"name\": null, \"columns\": null},\n" +
                    "    {\n" +
                    "      \"name\": \"t2\",\n" +
                    "      \"columns\": [\n" +
                    "        null,\n" +
                    "        {\"name\": \"c_skip\", \"type\": null},\n" +
                    "        {\"name\": null, \"type\": \"int32\"}\n" +
                    "      ]\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}\n";

    @Test
    public void testLoadSchemasFromResources() {
        BenchmarkTableSchemas schemas = new BenchmarkTableSchemas(null);

        Set<String> dbNames = new HashSet<>(schemas.getDbNames());
        Assertions.assertEquals(Set.of("tpcds", "tpch", "ssb"), dbNames);

        Assertions.assertTrue(schemas.getTableNames("tpcds").contains("date_dim"));
        Assertions.assertTrue(schemas.getTableNames("missing").isEmpty());
        Assertions.assertNull(schemas.getTableSchema("missing", "table"));

        List<Column> dateDim = schemas.getTableSchema("tpcds", "date_dim");
        Assertions.assertNotNull(dateDim);
        Assertions.assertEquals(DateType.DATE, findColumn(dateDim, "d_date").getType());
        Assertions.assertEquals(BooleanType.BOOLEAN, findColumn(dateDim, "d_holiday").getType());

        List<Column> callCenter = schemas.getTableSchema("tpcds", "call_center");
        Assertions.assertNotNull(callCenter);
        Assertions.assertEquals(FloatType.FLOAT, findColumn(callCenter, "cc_gmt_offset").getType());
        ScalarType taxType = (ScalarType) findColumn(callCenter, "cc_tax_percentage").getType();
        Assertions.assertEquals(5, taxType.decimalPrecision());
        Assertions.assertEquals(2, taxType.decimalScale());

        List<Column> part = schemas.getTableSchema("tpch", "part");
        Assertions.assertNotNull(part);
        Assertions.assertEquals(IntegerType.BIGINT, findColumn(part, "p_partkey").getType());
    }

    @Test
    public void testLoadSchemasFromPath(@TempDir Path tempDir) throws Exception {
        writeSchema(tempDir, "tpcds_schema.json", CUSTOM_SCHEMA_JSON);
        writeSchema(tempDir, "tpch_schema.json", CUSTOM_SCHEMA_JSON);
        writeSchema(tempDir, "ssb_schema.json", CUSTOM_SCHEMA_JSON);

        BenchmarkTableSchemas schemas = new BenchmarkTableSchemas(tempDir.toString());

        List<Column> columns = schemas.getTableSchema("TPCDS", "T1");
        Assertions.assertNotNull(columns);
        Assertions.assertEquals(8, columns.size());
        Assertions.assertEquals(IntegerType.INT, findColumn(columns, "c_int32").getType());
        Assertions.assertEquals(IntegerType.BIGINT, findColumn(columns, "c_int64").getType());

        Type stringType = findColumn(columns, "c_string").getType();
        Assertions.assertTrue(stringType.isStringType());

        Assertions.assertEquals(BooleanType.BOOLEAN, findColumn(columns, "c_bool").getType());
        Assertions.assertEquals(FloatType.FLOAT, findColumn(columns, "c_float").getType());
        Assertions.assertEquals(DateType.DATE, findColumn(columns, "c_date32").getType());
        Assertions.assertEquals(DateType.DATE, findColumn(columns, "c_date32_day").getType());

        ScalarType decimalType = (ScalarType) findColumn(columns, "c_decimal").getType();
        Assertions.assertEquals(10, decimalType.decimalPrecision());
        Assertions.assertEquals(2, decimalType.decimalScale());

        List<Column> emptyTable = schemas.getTableSchema("tpcds", "t2");
        Assertions.assertNotNull(emptyTable);
        Assertions.assertTrue(emptyTable.isEmpty());
    }

    @Test
    public void testLoadSchemasMissingFile(@TempDir Path tempDir) {
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new BenchmarkTableSchemas(tempDir.toString()));
    }

    @Test
    public void testLoadSchemasInvalidSchema(@TempDir Path tempDir) throws Exception {
        String invalidJson = "{}";
        writeSchema(tempDir, "tpcds_schema.json", invalidJson);
        writeSchema(tempDir, "tpch_schema.json", invalidJson);
        writeSchema(tempDir, "ssb_schema.json", invalidJson);

        StarRocksConnectorException exception = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new BenchmarkTableSchemas(tempDir.toString()));
        Assertions.assertTrue(exception.getMessage().contains("Invalid benchmark schema"));
    }

    @Test
    public void testLoadSchemasUnsupportedType(@TempDir Path tempDir) throws Exception {
        String unsupportedJson = "{\n" +
                "  \"tables\": [\n" +
                "    {\"name\": \"t1\", \"columns\": [{\"name\": \"c1\", \"type\": \"unsupported\"}]}\n" +
                "  ]\n" +
                "}\n";
        writeSchema(tempDir, "tpcds_schema.json", unsupportedJson);
        writeSchema(tempDir, "tpch_schema.json", unsupportedJson);
        writeSchema(tempDir, "ssb_schema.json", unsupportedJson);

        StarRocksConnectorException exception = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new BenchmarkTableSchemas(tempDir.toString()));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported benchmark type"));
    }

    private static Column findColumn(List<Column> columns, String name) {
        return columns.stream()
                .filter(column -> column.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Missing column " + name));
    }

    private static void writeSchema(Path dir, String fileName, String json) throws Exception {
        Files.writeString(dir.resolve(fileName), json, StandardCharsets.UTF_8);
    }
}
