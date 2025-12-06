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

package com.starrocks.connector.redis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.RedisTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import io.airlift.json.JsonCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;

public class RedisMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(RedisMetadata.class);

    private final Map<String, String> properties;
    private final File tableDescriptionDir;
    private final String defaultSchema;
    private final String catalogName;
    private final JsonCodec<RedisTableDescription> tableDescriptionCodec;

    public RedisMetadata(Map<String, String> properties, String catalogName) {
        this.properties = properties;
        this.tableDescriptionDir = new File(properties.get("redis.table-description-dir"));
        this.defaultSchema = properties.get("redis.default-schema");
        this.catalogName = catalogName;
        this.tableDescriptionCodec = JsonCodec.jsonCodec(RedisTableDescription.class);
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        for (File file : listFiles(tableDescriptionDir)) {
            if (file.isFile() && file.getName().endsWith(".json")) {
                RedisTableDescription table = null;
                try {
                    table = tableDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                String schemaName = firstNonNull(table.getSchemaName(), defaultSchema);
                schemaNames.add(schemaName);
                LOG.debug("Redis table %s.%s: %s", schemaName, table.getTableName(), table);
            }
        }
        return Lists.newArrayList(schemaNames.build());
    }

    @Override
    public Database getDb(ConnectContext context, String name) {
        try {
            if (listDbNames(context).contains(name)) {
                return new Database(0, name);
            } else {
                return null;
            }
        } catch (StarRocksConnectorException e) {
            return null;
        }
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
        for (File file : listFiles(tableDescriptionDir)) {
            if (file.isFile() && file.getName().endsWith(".json")) {
                RedisTableDescription table = null;
                try {
                    table = tableDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (table.getSchemaName().equalsIgnoreCase(dbName)) {
                    String tableName = firstNonNull(table.getTableName(), defaultSchema);
                    tableNames.add(tableName);
                    LOG.debug("Redis table %s.%s: %s", tableName, table.getTableName(), table);
                }
            }
        }
        return Lists.newArrayList(tableNames.build());
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        RedisTableDescription table = null;
        for (File file : listFiles(tableDescriptionDir)) {
            if (file.isFile() && file.getName().endsWith(".json")) {
                try {
                    table = tableDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (table.getSchemaName().equalsIgnoreCase(dbName) && table.getTableName().equalsIgnoreCase(tblName)) {
                    LOG.debug("Redis table %s.%s: %s", dbName, tblName, table);
                    return toRedisTable(table, properties, tblName, dbName, catalogName, table.getValue().getDataFormat());
                }
            }
        }
        return null;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.REDIS;
    }

    private static List<File> listFiles(File dir) {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                LOG.debug("Considering files: %s", asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    public static RedisTable toRedisTable(RedisTableDescription table, Map<String, String> properties,
                                          String tableName, String dbName, String catalogName, String valueDataFormat) {

        List<Column> columns = RedisUtil.convertColumnSchema(table);

        return new RedisTable(CONNECTOR_ID_GENERATOR.getNextId().asInt(),
                catalogName, dbName, tableName, columns, properties, valueDataFormat);
    }
}
