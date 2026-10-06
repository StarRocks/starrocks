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

package com.starrocks.connector.lance;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.LanceTable;
import com.starrocks.type.ArrayType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.starrocks.type.BooleanType.BOOLEAN;
import static com.starrocks.type.DateType.DATETIME;
import static com.starrocks.type.FloatType.DOUBLE;
import static com.starrocks.type.FloatType.FLOAT;
import static com.starrocks.type.IntegerType.BIGINT;
import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.IntegerType.SMALLINT;
import static com.starrocks.type.VarbinaryType.VARBINARY;
import static com.starrocks.type.VarcharType.VARCHAR;

public class LanceMetadataTest {

    @Test
    public void testTypeParsing() {
        // Scalar types
        Assertions.assertEquals(BOOLEAN, LanceApiConverter.parseType("boolean"));
        Assertions.assertEquals(INT, LanceApiConverter.parseType("int32"));
        Assertions.assertEquals(BIGINT, LanceApiConverter.parseType("int64"));
        Assertions.assertEquals(FLOAT, LanceApiConverter.parseType("float32"));
        Assertions.assertEquals(DOUBLE, LanceApiConverter.parseType("float64"));
        Assertions.assertEquals(VARCHAR, LanceApiConverter.parseType("string"));
        Assertions.assertEquals(DATETIME, LanceApiConverter.parseType("timestamp[us]"));
        Assertions.assertEquals(DATETIME, LanceApiConverter.parseType("timestamp[us, tz=UTC]"));

        Assertions.assertEquals(SMALLINT, LanceApiConverter.parseType("uint8"));
        Assertions.assertEquals(INT, LanceApiConverter.parseType("uint16"));
        Assertions.assertEquals(BIGINT, LanceApiConverter.parseType("uint32"));
        Assertions.assertEquals(TypeFactory.createUnifiedDecimalType(20, 0), LanceApiConverter.parseType("uint64"));
        Assertions.assertEquals(VARCHAR, LanceApiConverter.parseType("large_string"));
        Assertions.assertEquals(VARCHAR, LanceApiConverter.parseType("large_utf8"));
        Assertions.assertEquals(VARBINARY, LanceApiConverter.parseType("large_binary"));

        // Nested types
        Type arrayType = LanceApiConverter.parseType("list<float32>");
        Assertions.assertTrue(arrayType.isArrayType());
        Assertions.assertEquals(FLOAT, ((ArrayType) arrayType).getItemType());

        Type arrowArrayType = LanceApiConverter.parseType("list<item: int32>");
        Assertions.assertTrue(arrowArrayType.isArrayType());
        Assertions.assertEquals(INT, ((ArrayType) arrowArrayType).getItemType());

        // Vector embeddings as fixed_size_list
        Type vectorType = LanceApiConverter.parseType("fixed_size_list<float32, 128>");
        Assertions.assertTrue(vectorType.isArrayType());
        Assertions.assertEquals(FLOAT, ((ArrayType) vectorType).getItemType());

        Type arrowVectorType = LanceApiConverter.parseType("fixed_size_list<item: float>[128]");
        Assertions.assertTrue(arrowVectorType.isArrayType());
        Assertions.assertEquals(FLOAT, ((ArrayType) arrowVectorType).getItemType());

        Type lanceVectorType = LanceApiConverter.parseType("fixed_size_list:float:128");
        Assertions.assertTrue(lanceVectorType.isArrayType());
        Assertions.assertEquals(FLOAT, ((ArrayType) lanceVectorType).getItemType());

        // Struct types with case preservation
        Type structType = LanceApiConverter.parseType("struct<UserID:int64,embedding:fixed_size_list<float32,512>>");
        Assertions.assertTrue(structType.isStructType());
        com.starrocks.type.StructType sType = (com.starrocks.type.StructType) structType;
        Assertions.assertEquals(2, sType.getFields().size());
        Assertions.assertEquals("UserID", sType.getFields().get(0).getName());
        Assertions.assertEquals("embedding", sType.getFields().get(1).getName());
    }

    @Test
    public void testDirectoryDiscoveryAndSchema() {
        LanceDirectoryCatalog directory = org.mockito.Mockito.mock(LanceDirectoryCatalog.class);
        org.mockito.Mockito.when(directory.listTables("s3://bucket/warehouse", "{}"))
                .thenReturn("[\"users\",\"events\"]");
        org.mockito.Mockito.when(directory.describeTable("s3://bucket/warehouse", "{}", "users"))
                .thenReturn("{\"location\":\"s3://bucket/warehouse/users.lance\",\"schema\":" + SCHEMA + "}");
        LanceMetadata metadata = new LanceMetadata("lance_catalog", java.util.Map.of(
                "lance.catalog.warehouse", "s3://bucket/warehouse",
                "lance.namespace.root_database", "datasets"), directory);
        Assertions.assertEquals(List.of("datasets"), metadata.listDbNames(null));
        Assertions.assertEquals(List.of("events", "users"), metadata.listTableNames(null, "datasets"));
        Assertions.assertTrue(metadata.listTableNames(null, "missing").isEmpty());
        Assertions.assertNull(metadata.getTable(null, "missing", "users"));
        Assertions.assertNull(metadata.getTable(null, "datasets", "missing"));
        LanceTable users = (LanceTable) metadata.getTable(null, "datasets", "USERS");
        Assertions.assertEquals("lance_catalog", users.getCatalogName());
        Assertions.assertEquals("datasets", users.getCatalogDBName());
        Assertions.assertEquals("datasets", users.toThrift(List.of()).getDbName());
        Assertions.assertEquals("s3://bucket/warehouse/users.lance", users.getUri());
        Assertions.assertEquals(BIGINT, users.getColumn("ID").getType());
        Assertions.assertFalse(users.getColumn("ID").isAllowNull());
        Assertions.assertTrue(users.getColumn("label").isAllowNull());
        Assertions.assertTrue(users.isSupported());
        Assertions.assertEquals(users.getId(), metadata.getTable(null, "datasets", "users").getId());
        // Re-read dataset metadata, rather than keeping a stale configured schema.
        org.mockito.Mockito.verify(directory, org.mockito.Mockito.times(2))
                .describeTable("s3://bucket/warehouse", "{}", "users");
    }

    private static final String SCHEMA = """
            {"fields":[
              {"name":"ID","nullable":false,"type":{"name":"int","bitWidth":64,"isSigned":true},"children":[]},
              {"name":"label","nullable":true,"type":{"name":"utf8"},"children":[]}
            ]}
            """;

    @Test
    public void testRejectInvalidConfiguration() {
        for (java.util.Map<String, String> properties : List.of(
                java.util.Map.<String, String>of(),
                java.util.Map.of("lance.catalog.type", "rest", "lance.catalog.warehouse", "s3://bucket/root"),
                java.util.Map.of("lance.catalog.warehouse", "relative/path"),
                java.util.Map.of("lance.catalog.warehouse", "s3://bucket/root?sig=secret"),
                java.util.Map.of("lance.catalog.warehouse", "/tmp/data", "lance.namespace.root_database", " "),
                java.util.Map.of("lance.catalog.warehouse", "/tmp/data", "table.rows.schema", "id:int32"))) {
            Assertions.assertThrows(com.starrocks.connector.exception.StarRocksConnectorException.class,
                    () -> new LanceMetadata("lance", properties));
        }
    }

    @Test
    public void testCatalogIdentityAndAzureSasConfiguration() {
        LanceMetadata metadata = new LanceMetadata("azure_lance", java.util.Map.of(
                "lance.catalog.warehouse", "abfss://data@account.dfs.core.windows.net/warehouse",
                "azure.adls2.storage_account", "account", "azure.adls2.sas_token", "sig=test-sas"));
        com.starrocks.thrift.TCloudConfiguration thrift = new com.starrocks.thrift.TCloudConfiguration();
        metadata.getCloudConfiguration().toThrift(thrift);
        Assertions.assertEquals(com.starrocks.thrift.TCloudType.AZURE, thrift.getCloud_type());
        Assertions.assertEquals("sig=test-sas",
                thrift.getCloud_properties().get("fs.azure.sas.fixed.token.account.dfs.core.windows.net"));
    }

    @Test
    public void testStorageErrorsAreNotMissingTables() {
        LanceDirectoryCatalog directory = org.mockito.Mockito.mock(LanceDirectoryCatalog.class);
        org.mockito.Mockito.when(directory.listTables("/tmp/warehouse", "{}"))
                .thenThrow(new com.starrocks.connector.exception.StarRocksConnectorException("Storage access denied"));
        LanceMetadata metadata = new LanceMetadata("lance", java.util.Map.of("lance.catalog.warehouse", "/tmp/warehouse"),
                directory);
        Assertions.assertThrows(com.starrocks.connector.exception.StarRocksConnectorException.class,
                () -> metadata.getTable(null, "default", "rows"));
    }

    @Test
    public void testCompleteSchemaConversion() {
        String schema = """
                {"fields":[
                  {"name":"vector","nullable":true,"type":{"name":"fixedsizelist","listSize":3},"children":[
                    {"name":"item","nullable":true,"type":{"name":"floatingpoint","precision":"SINGLE"}}]},
                  {"name":"price","nullable":true,"type":{"name":"decimal","bitWidth":128,"precision":20,"scale":2}},
                  {"name":"u64","nullable":true,"type":{"name":"int","bitWidth":64,"isSigned":false}},
                  {"name":"ts","nullable":true,"type":{"name":"timestamp","unit":"MICROSECOND","timezone":"UTC"}}
                ]}
                """;
        List<Column> columns = LanceApiConverter.fromSchema(com.google.gson.JsonParser.parseString(schema).getAsJsonObject());
        Assertions.assertEquals(FLOAT, ((ArrayType) columns.get(0).getType()).getItemType());
        Assertions.assertEquals(TypeFactory.createUnifiedDecimalType(20, 2), columns.get(1).getType());
        Assertions.assertEquals(TypeFactory.createUnifiedDecimalType(20, 0), columns.get(2).getType());
        Assertions.assertEquals(DATETIME, columns.get(3).getType());
        Assertions.assertThrows(com.starrocks.connector.exception.StarRocksConnectorException.class,
                () -> LanceApiConverter.fromSchema(com.google.gson.JsonParser.parseString(
                        schema.replace("fixedsizelist", "union")).getAsJsonObject()));
    }
}
