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

package com.starrocks.connector.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KuduTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HivePartitionStats;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.RpcRemoteException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class KuduMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(KuduMetadata.class);
    private final String catalogName;
    private final HdfsEnvironment hdfsEnvironment;
    private final String masterAddresses;
    private final KuduClient kuduClient;
    private final Optional<IHiveMetastore> metastore;
    private final Boolean schemaEmulationEnabled;
    private final String schemaEmulationPrefix;
    private static final String DATABASE_TABLE_JOINER = ".";
    public static final String DEFAULT_SCHEMA = "default";
    public static final Set<String> HIVE_SYSTEM_SCHEMA = Sets.newHashSet("information_schema", "sys");
    private static final ConcurrentHashMap<String, KuduClient> KUDU_CLIENTS = new ConcurrentHashMap<>();
    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final Map<String, Database> databases = new ConcurrentHashMap<>();

    public KuduMetadata(String catalogName, HdfsEnvironment hdfsEnvironment, String master, boolean schemaEmulationEnabled,
                        String schemaEmulationPrefix, Optional<IHiveMetastore> hiveMetastore) {
        this.masterAddresses = master;
        this.catalogName = catalogName;
        this.hdfsEnvironment = hdfsEnvironment;
        this.metastore = hiveMetastore;
        this.kuduClient = KUDU_CLIENTS.computeIfAbsent(master, m -> new KuduClient.KuduClientBuilder(m).build());
        this.schemaEmulationEnabled = schemaEmulationEnabled;
        this.schemaEmulationPrefix = schemaEmulationPrefix;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        if (metastore.isPresent()) {
            return metastore.get().getAllDatabaseNames().stream()
                    .filter(schemaName -> !HIVE_SYSTEM_SCHEMA.contains((schemaName)))
                    .collect(toImmutableList());
        }
        if (!schemaEmulationEnabled) {
            return Collections.singletonList(DEFAULT_SCHEMA);
        }
        try {
            List<String> tables = kuduClient.getTablesList().getTablesList();
            LinkedHashSet<String> schemas = new LinkedHashSet<>();
            schemas.add(DEFAULT_SCHEMA);
            for (String table : tables) {
                Optional.ofNullable(fromRawName(table)).ifPresent(p -> schemas.add(p.first));
            }
            return ImmutableList.copyOf(schemas);
        } catch (KuduException e) {
            throw new StarRocksConnectorException("Failed to list databases", e);
        }
    }

    public Pair<String, String> fromRawName(String rawName) {
        if (schemaEmulationPrefix.isEmpty()) {
            int dotIndex = rawName.indexOf('.');
            if (dotIndex == -1) {
                return Pair.create(DEFAULT_SCHEMA, rawName);
            }
            if (dotIndex == 0 || dotIndex == rawName.length() - 1) {
                return null; // illegal rawName ignored
            }
            return Pair.create(rawName.substring(0, dotIndex), rawName.substring(dotIndex + 1));
        }
        if (rawName.startsWith(schemaEmulationPrefix)) {
            int start = schemaEmulationPrefix.length();
            int dotIndex = rawName.indexOf('.', start);
            if (dotIndex == -1 || dotIndex == start || dotIndex == rawName.length() - 1) {
                return null; // illegal rawName ignored
            }
            String schema = rawName.substring(start, dotIndex);
            if (DEFAULT_SCHEMA.equalsIgnoreCase(schema)) {
                return null; // illegal rawName ignored
            }
            return Pair.create(schema, rawName.substring(dotIndex + 1));
        }
        return Pair.create(DEFAULT_SCHEMA, rawName);
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        if (metastore.isPresent()) {
            List<String> allTableNames = metastore.get().getAllTableNames(dbName);
            return allTableNames.stream().filter(tableName -> {
                Table table = metastore.get().getTable(dbName, tableName);
                return table.isKuduTable();
            }).collect(Collectors.toList());
        }
        try {
            List<String> tablesList = kuduClient.getTablesList().getTablesList();
            if (!schemaEmulationEnabled && DEFAULT_SCHEMA.equals(dbName)) {
                return tablesList;
            } else if (schemaEmulationEnabled) {
                return tablesList.stream()
                        .flatMap(t -> {
                            Pair<String, String> db2table = fromRawName(t);
                            if (db2table != null && dbName.equals(db2table.first)) {
                                return ImmutableList.of(db2table.second).stream();
                            }
                            return ImmutableList.<String>of().stream();
                        })
                        .collect(Collectors.toList());
            }
            throw new StarRocksConnectorException("Database %s not exists", dbName);
        } catch (KuduException e) {
            throw new StarRocksConnectorException("Database %s not exists", dbName, e);
        }
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        if (metastore.isPresent()) {
            return metastore.get().getDb(dbName);
        }
        if (!schemaEmulationEnabled) {
            if (DEFAULT_SCHEMA.equals(dbName)) {
                return databases.computeIfAbsent(DEFAULT_SCHEMA,
                        d -> new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), d));
            }
            return null;
        }
        try {
            List<String> tables = kuduClient.getTablesList().getTablesList();
            if (tables.stream().anyMatch(table -> {
                Pair<String, String> schemaTable = fromRawName(table);
                return schemaTable != null && dbName.equals(schemaTable.first);
            })) {
                return databases.computeIfAbsent(dbName,
                        d -> new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), d));
            }
            LOG.error("Kudu database {}.{} done not exist.", catalogName, dbName);
            return null;
        } catch (KuduException e) {
            throw new StarRocksConnectorException("Failed to get database %s", dbName, e);
        }
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        if (metastore.isPresent()) {
            return metastore.get().getTable(dbName, tblName);
        }
        String fullTableName = getKuduFullTableName(dbName, tblName);
        try {
            if (!kuduClient.tableExists(fullTableName)) {
                LOG.error("Kudu table {}.{} does not exist.", dbName, tblName);
                return null;
            }
            return tables.computeIfAbsent(fullTableName,
                    f -> {
                        org.apache.kudu.client.KuduTable kuduNativeTable;
                        try {
                            kuduNativeTable = kuduClient.openTable(fullTableName);
                        } catch (KuduException e) {
                            throw new StarRocksConnectorException("Failed to open table %s.", fullTableName, e);
                        }
                        List<ColumnSchema> columnSchemas = kuduNativeTable.getSchema().getColumns();
                        ArrayList<Column> columns = new ArrayList<>(columnSchemas.size());
                        Set<Integer> parColumnIds =
                                Sets.newHashSet(kuduNativeTable.getPartitionSchema().getRangeSchema().getColumnIds());
                        List<String> partColNames = Lists.newArrayList();
                        for (int i = 0; i < columnSchemas.size(); i++) {
                            ColumnSchema columnSchema = columnSchemas.get(i);
                            String fieldName = columnSchema.getName();
                            boolean isKey = columnSchema.isKey();
                            String comment = columnSchema.getComment();
                            Type fieldType = ColumnTypeConverter.fromKuduType(columnSchema);
                            Column column = new Column(fieldName, fieldType, isKey,
                                    null, true, null, comment);
                            columns.add(column);
                            if (parColumnIds.contains(i)) {
                                partColNames.add(fieldName);
                            }
                        }
                        return new KuduTable(masterAddresses, this.catalogName, dbName, tblName, fullTableName,
                                columns, partColNames);
                    });
        } catch (KuduException e) {
            throw new StarRocksConnectorException("Failed to get table %s.", fullTableName, e);
        }
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.KUDU;
    }

    private String getKuduFullTableName(String dbName, String tblName) {
        return schemaEmulationEnabled ? (schemaEmulationPrefix + dbName + DATABASE_TABLE_JOINER + tblName) : tblName;
    }

    private String getKuduFullTableName(KuduTable table) {
        return table.getKuduTableName().orElse(
                getKuduFullTableName(table.getCatalogDBName(), table.getCatalogTableName()));
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        KuduTable kuduTable = (KuduTable) table;
        String kuduTableName = getKuduFullTableName(kuduTable);
        org.apache.kudu.client.KuduTable nativeTable;
        try {
            nativeTable = kuduClient.openTable(kuduTableName);
        } catch (KuduException e) {
            throw new RuntimeException(e);
        }
        KuduScanToken.KuduScanTokenBuilder builder = kuduClient.newScanTokenBuilder(nativeTable);
        builder.setProjectedColumnNames(params.getFieldNames());
        if (params.getLimit() > 0) {
            builder.limit(params.getLimit());
        }
        addConstraintPredicates(nativeTable, builder, params.getPredicate());
        List<KuduScanToken> tokens = builder.build();
        List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(
                KuduRemoteFileDesc.createKuduRemoteFileDesc(tokens));
        remoteFileInfo.setFiles(remoteFileDescs);
        return Lists.newArrayList(remoteFileInfo);
    }

    private void addConstraintPredicates(org.apache.kudu.client.KuduTable table,
                                         KuduScanToken.KuduScanTokenBuilder builder,
                                         ScalarOperator predicate) {
        List<ScalarOperator> scalarOperators = Utils.extractConjuncts(predicate);
        if (!scalarOperators.isEmpty()) {
            KuduPredicateConverter converter = new KuduPredicateConverter(table.getSchema());
            for (ScalarOperator operator : scalarOperators) {
                List<KuduPredicate> kuduPredicates = converter.convert(operator);
                if (kuduPredicates != null && !kuduPredicates.isEmpty()) {
                    kuduPredicates.forEach(builder::addPredicate);
                }
            }
        }
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return hdfsEnvironment.getCloudConfiguration();
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit,
                                         TableVersionRange versionRange) {
        Statistics.Builder builder = Statistics.builder();
        for (ColumnRefOperator columnRefOperator : columns.keySet()) {
            builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
        }

        KuduTable kuduTable = (KuduTable) table;
        long rowCount;
        if (metastore.isPresent()) {
            HivePartitionStats tableStatistics =
                    metastore.get().getTableStatistics(kuduTable.getCatalogDBName(), kuduTable.getCatalogTableName());
            rowCount = tableStatistics.getCommonStats().getRowNums();
        } else {
            try {
                String kuduTableName = getKuduFullTableName(kuduTable);
                rowCount = kuduClient.openTable(kuduTableName).getTableStatistics().getLiveRowCount();
            } catch (RpcRemoteException e) {
                if (isGetTableStatisticsUnsupported(e)) {
                    LOG.warn("GetTableStatistics method not supported. Fallback to return default row count 1.");
                    rowCount = 1;
                } else {
                    throw new RuntimeException("RPC error while getting table statistics", e);
                }
            } catch (KuduException e) {
                throw new RuntimeException("Error while accessing Kudu table", e);
            }
        }
        builder.setOutputRowCount(rowCount);
        return builder.build();
    }

    private static boolean isGetTableStatisticsUnsupported(RpcRemoteException e) {
        return e.getMessage().contains("invalid method name: GetTableStatistics");
    }
}
