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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LanceTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.lance.LanceConfig;
import com.starrocks.qe.ConnectContext;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.ReadOptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class LanceMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(LanceMetadata.class);

    private final String catalogName;
    private final Map<String, String> properties;
    private final HdfsEnvironment hdfsEnvironment;
    private final String warehousePath;
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<String, Table> tables = new ConcurrentHashMap<>();

    public LanceMetadata(String catalogName, Map<String, String> properties, HdfsEnvironment hdfsEnvironment) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.hdfsEnvironment = hdfsEnvironment;
        this.warehousePath = stripTrailingSlash(properties.get(LanceConnector.LANCE_CATALOG_WAREHOUSE));
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.LANCE;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        Set<String> dbNames = new LinkedHashSet<>();
        dbNames.add(LanceConnector.DEFAULT_DB);
        for (FileStatus status : listStatus(warehousePath)) {
            String name = status.getPath().getName();
            if (status.isDirectory() && !name.endsWith(LanceConfig.LANCE_FILE_SUFFIX)) {
                dbNames.add(name);
            }
        }
        return ImmutableList.copyOf(dbNames);
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        return databases.computeIfAbsent(dbName,
                name -> new Database(CONNECTOR_ID_GENERATOR.getNextId().asLong(), name));
    }

    @Override
    public boolean dbExists(ConnectContext context, String dbName) {
        return dbName != null && !dbName.isEmpty();
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        Set<String> tableNames = new LinkedHashSet<>();
        if (LanceConnector.DEFAULT_DB.equalsIgnoreCase(dbName)) {
            tableNames.addAll(listTableNamesInPath(warehousePath));
            tableNames.addAll(listTableNamesInPath(joinPath(warehousePath, dbName)));
        } else {
            tableNames.addAll(listTableNamesInPath(joinPath(warehousePath, dbName)));
        }
        return ImmutableList.copyOf(tableNames);
    }

    private List<String> listTableNamesInPath(String tableRoot) {
        return listStatus(tableRoot).stream()
                .filter(FileStatus::isDirectory)
                .map(status -> status.getPath().getName())
                .filter(name -> name.endsWith(LanceConfig.LANCE_FILE_SUFFIX))
                .map(name -> name.substring(0, name.length() - LanceConfig.LANCE_FILE_SUFFIX.length()))
                .collect(Collectors.toList());
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        String cacheKey = dbName.toLowerCase(Locale.ROOT) + "." + tblName.toLowerCase(Locale.ROOT);
        Table cached = tables.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        RuntimeException lastException = null;
        for (String datasetUri : candidateDatasetUris(dbName, tblName)) {
            try {
                LanceTable table = new LanceTable(catalogName, dbName, tblName, inferSchema(datasetUri),
                        buildTableProperties(datasetUri));
                tables.put(cacheKey, table);
                return table;
            } catch (RuntimeException e) {
                lastException = e;
                LOG.debug("Failed to open lance dataset {}", datasetUri, e);
            }
        }
        if (lastException != null) {
            throw lastException;
        }
        return null;
    }

    @Override
    public boolean tableExists(ConnectContext context, String dbName, String tblName) {
        try {
            return getTable(context, dbName, tblName) != null;
        } catch (Exception e) {
            return false;
        }
    }

    private List<String> candidateDatasetUris(String dbName, String tblName) {
        List<String> uris = new ArrayList<>();
        if (LanceConnector.DEFAULT_DB.equalsIgnoreCase(dbName)) {
            uris.add(joinPath(warehousePath, tblName + LanceConfig.LANCE_FILE_SUFFIX));
            uris.add(joinPath(joinPath(warehousePath, dbName), tblName + LanceConfig.LANCE_FILE_SUFFIX));
        } else {
            uris.add(joinPath(joinPath(warehousePath, dbName), tblName + LanceConfig.LANCE_FILE_SUFFIX));
        }
        return uris;
    }

    private Map<String, String> buildTableProperties(String datasetUri) {
        Map<String, String> tableProperties = new HashMap<>(properties);
        tableProperties.put(LanceTable.DATASET_URI, datasetUri);
        return tableProperties;
    }

    private List<Column> inferSchema(String datasetUri) {
        ReadOptions.Builder builder = new ReadOptions.Builder();
        Map<String, String> storageOptions = LanceConfig.buildStorageOptions(properties);
        if (!storageOptions.isEmpty()) {
            builder.setStorageOptions(storageOptions);
        }

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Dataset dataset = Dataset.open(allocator, datasetUri, builder.build())) {
            return LanceSchemaConverter.fromArrowSchema(dataset.getSchema());
        } catch (Exception e) {
            throw new StarRocksConnectorException("Failed to open lance dataset %s: %s", datasetUri,
                    ExceptionUtils.getRootCauseMessage(e));
        }
    }

    private List<FileStatus> listStatus(String path) {
        try {
            Path hadoopPath = new Path(path);
            FileSystem fileSystem = FileSystem.get(hadoopPath.toUri(), hdfsEnvironment.getConfiguration());
            FileStatus[] statuses = fileSystem.listStatus(hadoopPath);
            if (statuses == null) {
                return List.of();
            }
            return List.of(statuses);
        } catch (Exception e) {
            LOG.debug("Failed to list lance path {}", path, e);
            return List.of();
        }
    }

    private static String joinPath(String parent, String child) {
        if (parent.endsWith("/")) {
            return parent + child;
        }
        return parent + "/" + child;
    }

    private static String stripTrailingSlash(String path) {
        if (path != null && path.endsWith("/")) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }
}
