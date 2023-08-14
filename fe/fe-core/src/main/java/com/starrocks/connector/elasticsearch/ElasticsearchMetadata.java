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

package com.starrocks.connector.elasticsearch;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

// TODO add meta cache
public class ElasticsearchMetadata
        implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(EsTable.class);

    private final EsRestClient esRestClient;
    private final Map<String, String> properties;
    private final String catalogName;
    public static final String DEFAULT_DB = "default_db";
    public static final long DEFAULT_DB_ID = 1L;


    public ElasticsearchMetadata(EsRestClient esRestClient, Map<String, String> properties, String catalogName) {
        this.esRestClient = esRestClient;
        this.properties = properties;
        this.catalogName = catalogName;
    }

    @Override
    public List<String> listDbNames() {
        return Arrays.asList(DEFAULT_DB);
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return esRestClient.listTables();
    }

    @Override
    public Database getDb(String dbName) {
        return new Database(DEFAULT_DB_ID, DEFAULT_DB);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        if (!DEFAULT_DB.equalsIgnoreCase(dbName)) {
            return null;
        }
        return toEsTable(esRestClient, properties, tblName, dbName, catalogName);
    }

    public static EsTable toEsTable(EsRestClient esRestClient,
                                    Map<String, String> properties,
                                    String tableName, String dbName, String catalogName) {
        try {
            List<Column> columns = EsUtil.convertColumnSchema(esRestClient, tableName);
            properties.put(EsTable.KEY_INDEX, tableName);
            EsTable esTable = new EsTable(CONNECTOR_ID_GENERATOR.getNextId().asInt(),
                    catalogName, dbName, tableName, columns, properties, new SinglePartitionInfo());
            esTable.setComment("created by external es catalog");
            esTable.syncTableMetaData(esRestClient);
            return esTable;
        } catch (NoSuchElementException e) {
            LOG.error(String.format("Unknown index {%s}", tableName), e);
            return null;
        } catch (Exception e) {
            LOG.error("transform to EsTable Error", e);
            return null;
        }

    }
}