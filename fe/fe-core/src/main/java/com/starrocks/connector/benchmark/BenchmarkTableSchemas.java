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

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class BenchmarkTableSchemas {
    private static final Pattern DECIMAL_PATTERN =
            Pattern.compile("decimal\\d*\\((\\d+)\\s*,\\s*(\\d+)\\)");

    private final Map<String, Map<String, List<Column>>> schemasByDb;

    public BenchmarkTableSchemas(String schemaDir) {
        this.schemasByDb = loadSchemas(schemaDir);
    }

    public List<String> getDbNames() {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (BenchmarkSuite suite : BenchmarkSuiteFactory.getSuites()) {
            builder.add(BenchmarkNameUtils.normalizeDbName(suite.getName()));
        }
        return builder.build();
    }

    public List<String> getTableNames(String dbName) {
        Map<String, List<Column>> tables = schemasByDb.get(BenchmarkNameUtils.normalizeDbName(dbName));
        if (tables == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(tables.keySet());
    }

    public List<Column> getTableSchema(String dbName, String tableName) {
        Map<String, List<Column>> tables = schemasByDb.get(BenchmarkNameUtils.normalizeDbName(dbName));
        if (tables == null) {
            return null;
        }
        return tables.get(BenchmarkNameUtils.normalizeTableName(tableName));
    }

    private static Map<String, Map<String, List<Column>>> loadSchemas(String schemaDir) {
        if (schemaDir == null || schemaDir.isEmpty()) {
            return loadSchemasFromResources();
        }
        Path basePath = Paths.get(schemaDir);
        Map<String, Map<String, List<Column>>> schemas = new LinkedHashMap<>();
        for (BenchmarkSuite suite : BenchmarkSuiteFactory.getSuites()) {
            String suiteName = BenchmarkNameUtils.normalizeDbName(suite.getName());
            schemas.put(suiteName, loadSuiteSchemaFromPath(basePath.resolve(suite.getSchemaFileName()), suiteName));
        }
        return Collections.unmodifiableMap(schemas);
    }

    private static Map<String, Map<String, List<Column>>> loadSchemasFromResources() {
        Map<String, Map<String, List<Column>>> schemas = new LinkedHashMap<>();
        for (BenchmarkSuite suite : BenchmarkSuiteFactory.getSuites()) {
            String suiteName = BenchmarkNameUtils.normalizeDbName(suite.getName());
            schemas.put(suiteName, loadSuiteSchemaFromResource(suite.getSchemaResourcePath(), suiteName));
        }
        return Collections.unmodifiableMap(schemas);
    }

    private static Map<String, List<Column>> loadSuiteSchemaFromPath(Path schemaPath, String suiteName) {
        String json;
        try {
            json = Files.readString(schemaPath, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new StarRocksConnectorException(
                    "Failed to read benchmark schema for " + suiteName + " at " + schemaPath, e);
        }
        return parseSchema(json, suiteName, schemaPath.toString());
    }

    private static Map<String, List<Column>> loadSuiteSchemaFromResource(String resourcePath, String suiteName) {
        String json;
        try (InputStream inputStream = BenchmarkTableSchemas.class.getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                throw new StarRocksConnectorException(
                        "Missing benchmark schema resource for " + suiteName + " at " + resourcePath);
            }
            json = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new StarRocksConnectorException(
                    "Failed to read benchmark schema resource for " + suiteName + " at " + resourcePath, e);
        }
        return parseSchema(json, suiteName, resourcePath);
    }

    private static Map<String, List<Column>> parseSchema(String json, String suiteName, String source) {
        SchemaFile schemaFile = GsonUtils.GSON.fromJson(json, SchemaFile.class);
        if (schemaFile == null || schemaFile.tables == null) {
            throw new StarRocksConnectorException(
                    "Invalid benchmark schema for " + suiteName + " at " + source);
        }

        Map<String, List<Column>> tableSchemas = new LinkedHashMap<>();
        for (TableDef table : schemaFile.tables) {
            if (table == null || table.name == null || table.columns == null) {
                continue;
            }
            String tableName = BenchmarkNameUtils.normalizeTableName(table.name);
            List<Column> columns = new ArrayList<>();
            for (ColumnDef column : table.columns) {
                if (column == null || column.name == null || column.type == null) {
                    continue;
                }
                Type type = toColumnType(column.type);
                columns.add(new Column(column.name, type, true));
            }
            tableSchemas.put(tableName, ImmutableList.copyOf(columns));
        }
        return tableSchemas;
    }

    private static Type toColumnType(String rawType) {
        String type = rawType.trim().toLowerCase(Locale.ROOT);
        switch (type) {
            case "int32":
                return IntegerType.INT;
            case "int64":
                return IntegerType.BIGINT;
            case "string":
                return TypeFactory.createDefaultCatalogString();
            case "bool":
                return BooleanType.BOOLEAN;
            case "float":
                return FloatType.FLOAT;
            case "date32[day]":
            case "date32":
                return DateType.DATE;
            default:
                Matcher matcher = DECIMAL_PATTERN.matcher(type);
                if (matcher.matches()) {
                    int precision = Integer.parseInt(matcher.group(1));
                    int scale = Integer.parseInt(matcher.group(2));
                    return TypeFactory.createUnifiedDecimalType(precision, scale);
                }
                throw new StarRocksConnectorException("Unsupported benchmark type: " + rawType);
        }
    }

    private static final class SchemaFile {
        @SerializedName("tables")
        private List<TableDef> tables;
    }

    private static final class TableDef {
        @SerializedName("name")
        private String name;
        @SerializedName("columns")
        private List<ColumnDef> columns;
    }

    private static final class ColumnDef {
        @SerializedName("name")
        private String name;
        @SerializedName("type")
        private String type;
    }
}
