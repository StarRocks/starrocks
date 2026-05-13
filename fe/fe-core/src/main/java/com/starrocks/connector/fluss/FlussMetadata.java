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

package com.starrocks.connector.fluss;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FlussTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.Type;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.initializer.BucketOffsetsRetrieverImpl;
import org.apache.fluss.client.initializer.LatestOffsetsInitializer;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.lake.LakeSplitGenerator;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static org.apache.fluss.flink.utils.CatalogExceptionUtils.isTableNotExist;
import static org.apache.fluss.flink.utils.CatalogExceptionUtils.isTableNotPartitioned;
import static org.apache.fluss.flink.utils.LakeSourceUtils.createLakeSource;

public class FlussMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(FlussMetadata.class);

    private static final String LAKE_TABLE_SPLITTER = "$lake";
    public static final String RT_TABLE_SPLITTER = "$rt";

    private final Connection connection;
    private final Admin admin;

    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private final Map<TablePath, Table> tables = new ConcurrentHashMap<>();
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<TablePath, Map<String, Partition>> partitionInfos = new ConcurrentHashMap<>();
    private final Map<PredicateSearchKey, List<SourceSplitBase>> flussSplits = new ConcurrentHashMap<>();
    private final Map<String, String> tableProperties;

    public FlussMetadata(String catalogName, HdfsEnvironment hdfsEnvironment, Connection connection, Admin admin,
                         Map<String, String> tableProperties) {
        this.catalogName = catalogName;
        this.hdfsEnvironment = hdfsEnvironment;
        this.connection = connection;
        this.admin = admin;
        this.tableProperties = tableProperties;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.FLUSS;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        try {
            return this.admin.listDatabases().get();
        } catch (Exception e) {
            LOG.error("Failed to list databases {}.", catalogName, e);
            throw new StarRocksConnectorException(e.getMessage());
        }
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        try {
            return admin.listTables(dbName).get();
        } catch (Exception e) {
            LOG.error("Failed to list Fluss tables {}.{}.", catalogName, dbName, e);
            throw new StarRocksConnectorException(e.getMessage());
        }
    }

    private void updatePartitionInfo(String databaseName, String tableName) {
        try {
            TablePath identifier = TablePath.of(databaseName, tableName);
            if (!this.partitionInfos.containsKey(identifier)) {
                this.partitionInfos.put(identifier, new ConcurrentHashMap<>());
            }
            List<org.apache.fluss.metadata.PartitionInfo> flussPartitions =
                    admin.listPartitionInfos(identifier).get();
            for (org.apache.fluss.metadata.PartitionInfo partitionInfo : flussPartitions) {
                Partition srPartition = new Partition(partitionInfo.getPartitionName(), System.currentTimeMillis());
                this.partitionInfos.get(identifier).put(srPartition.getPartitionName(), srPartition);
            }
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (isTableNotExist(t)) {
                LOG.error("Failed to list Fluss partition names {}.{}.{} for TableNotExist.",
                        catalogName, databaseName, tableName, e);
                throw new StarRocksConnectorException(e.getMessage());
            } else if (isTableNotPartitioned(t)) {
                LOG.error("Failed to list Fluss partition names {}.{}.{} for TableNotPartitioned.",
                        catalogName, databaseName, tableName, e);
                throw new StarRocksConnectorException(e.getMessage());
            } else {
                LOG.error("Failed to list Fluss partition names {}.{}.{}.",
                        catalogName, databaseName, tableName, e);
                throw new StarRocksConnectorException(e.getMessage());
            }
        }
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName,
                                           ConnectorMetadatRequestContext requestContext) {
        TablePath identifier = TablePath.of(databaseName, tableName);
        updatePartitionInfo(databaseName, tableName);
        if (this.partitionInfos.get(identifier) == null) {
            return Lists.newArrayList();
        }
        return new ArrayList<>(this.partitionInfos.get(identifier).keySet());
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        if (this.databases.containsKey(dbName)) {
            return this.databases.get(dbName);
        }
        try {
            this.admin.getDatabaseInfo(dbName);
            Database db = new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), dbName);
            this.databases.put(dbName, db);
            return db;
        } catch (Exception e) {
            LOG.error("Failed to get Fluss database {}.{}.", catalogName, dbName, e);
            throw new StarRocksConnectorException(e.getMessage());
        }
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        TablePath cacheKey = TablePath.of(dbName, tblName);
        if (tables.containsKey(cacheKey)) {
            return tables.get(cacheKey);
        }

        String realTblName = tblName;
        TablePath flussIdentifier = TablePath.of(dbName, tblName);
        if (tblName.contains(LAKE_TABLE_SPLITTER)) {
            realTblName = tblName.split("\\" + LAKE_TABLE_SPLITTER)[0];
            flussIdentifier = TablePath.of(dbName, realTblName);
        }
        if (tblName.contains(RT_TABLE_SPLITTER)) {
            realTblName = tblName.split("\\" + RT_TABLE_SPLITTER)[0];
            flussIdentifier = TablePath.of(dbName, realTblName);
        }

        try {
            TableInfo tableInfo = this.admin.getTableInfo(flussIdentifier).get();
            List<Schema.Column> flussColumns = tableInfo.getSchema().getColumns();
            ArrayList<Column> fullSchema = new ArrayList<>(flussColumns.size());
            for (Schema.Column flussColumn : flussColumns) {
                String fieldName = flussColumn.getName();
                Type fieldType = ColumnTypeConverter.fromFlussType(flussColumn.getDataType());
                Column column = new Column(fieldName, fieldType, true, flussColumn.getComment().orElse(""));
                fullSchema.add(column);
            }
            String comment = tableInfo.getComment().orElse("");
            Configuration beConf = Configuration.fromMap(connection.getConfiguration().toMap());
            for (Map.Entry<String, String> e : tableInfo.getProperties().toMap().entrySet()) {
                beConf.setString(e.getKey(), e.getValue());
            }
            for (Map.Entry<String, String> e : tableProperties.entrySet()) {
                beConf.setString(e.getKey(), e.getValue());
            }
            FlussTable table = new FlussTable(catalogName, dbName, realTblName, fullSchema,
                    connection.getTable(flussIdentifier), beConf);
            table.setComment(comment);
            if (tblName.contains(LAKE_TABLE_SPLITTER)) {
                table.setTableNamePrefix(LAKE_TABLE_SPLITTER);
            }
            if (tblName.contains(RT_TABLE_SPLITTER)) {
                table.setTableNamePrefix(RT_TABLE_SPLITTER);
            }
            this.tables.put(cacheKey, table);
            return table;
        } catch (Exception e) {
            LOG.error("Failed to get Fluss table {}.{}.{}.", catalogName, dbName, tblName, e);
            throw new StarRocksConnectorException(e.getMessage());
        }
    }

    @Override
    public boolean tableExists(ConnectContext context, String dbName, String tableName) {
        try {
            TablePath identifier = TablePath.of(dbName, tableName);
            return admin.tableExists(identifier).get();
        } catch (Exception e) {
            LOG.warn("Failed to get Fluss table {}.{}.{}.", catalogName, dbName, tableName, e);
            return false;
        }
    }

    private List<org.apache.fluss.metadata.PartitionInfo> listFlussPartitions(Table table) {
        FlussTable flussTable = (FlussTable) table;
        TablePath identifier = TablePath.of(flussTable.getCatalogDBName(), flussTable.getCatalogTableName());
        try {
            return admin.listPartitionInfos(identifier).get();
        } catch (Exception e) {
            throw new StarRocksConnectorException(
                    String.format("Failed to list partitions for %s", identifier));
        }
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        FlussTable flussTable = (FlussTable) table;
        TablePath identifier = TablePath.of(flussTable.getCatalogDBName(), flussTable.getCatalogTableName());
        TableInfo tableInfo = flussTable.getTableInfo();

        OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever =
                new BucketOffsetsRetrieverImpl(admin, identifier);

        PredicateSearchKey filter = PredicateSearchKey.of(
                flussTable.getCatalogDBName(),
                flussTable.getCatalogTableName() + flussTable.getTableNamePrefix(),
                params);

        if (!flussSplits.containsKey(filter)) {
            Map<String, String> properties = new HashMap<>(flussTable.getTableInfo().getProperties().toMap());
            properties.putAll(tableProperties);

            Supplier<Set<org.apache.fluss.metadata.PartitionInfo>> listPartitionSupplier =
                    () -> new LinkedHashSet<>(listFlussPartitions(table));
            LakeSource<LakeSplit> lakeSource =
                    createLakeSource(flussTable.getTableInfo().getTablePath(), properties);
            if (lakeSource != null) {
                applyLakeSourceFilters(lakeSource, flussTable, params.getPredicate());
            }
            LakeSplitGenerator lakeSplitGenerator = new LakeSplitGenerator(
                    tableInfo, admin,
                    lakeSource, bucketOffsetsRetriever,
                    new LatestOffsetsInitializer(), tableInfo.getNumBuckets(), listPartitionSupplier);
            List<SourceSplitBase> splits = new ArrayList<>();
            try {
                splits = lakeSplitGenerator.generateHybridLakeFlussSplits();
                if (splits == null) {
                    splits = new ArrayList<>();
                }
            } catch (Exception e) {
                LOG.error("Failed to get Fluss splits for table {}.{}.{}.",
                        catalogName, flussTable.getCatalogDBName(), flussTable.getCatalogTableName(), e);
                throw new StarRocksConnectorException("Failed to get Fluss splits for table %s.%s.%s: %s",
                        catalogName, flussTable.getCatalogDBName(), flussTable.getCatalogTableName(), e.getMessage());
            }
            if (flussTable.getTableNamePrefix().equals(LAKE_TABLE_SPLITTER)) {
                splits = splits.stream().filter(sp -> sp instanceof LakeSnapshotSplit)
                        .collect(Collectors.toList());
            }
            if (flussTable.getTableNamePrefix().equals(RT_TABLE_SPLITTER)) {
                splits = splits.stream().filter(sp -> sp instanceof LogSplit)
                        .collect(Collectors.toList());
            }

            flussSplits.put(filter, splits);
            List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(
                    FlussRemoteFileDesc.createFlussRemoteFileDesc(splits));
            remoteFileInfo.setFiles(remoteFileDescs);
        } else {
            List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(
                    FlussRemoteFileDesc.createFlussRemoteFileDesc(flussSplits.get(filter)));
            remoteFileInfo.setFiles(remoteFileDescs);
        }

        return Lists.newArrayList(remoteFileInfo);
    }

    private void applyLakeSourceFilters(LakeSource<LakeSplit> lakeSource, FlussTable flussTable,
                                        ScalarOperator predicate) {
        RowType flussRowType = flussTable.getTableInfo().getRowType();
        FlussPredicateConverter lakeConverter = new FlussPredicateConverter(flussRowType);
        List<org.apache.fluss.predicate.Predicate> lakePredicates = new ArrayList<>();

        List<ScalarOperator> scalarOperators = Utils.extractConjuncts(predicate);
        for (ScalarOperator operator : scalarOperators) {
            org.apache.fluss.predicate.Predicate lakePredicate = lakeConverter.convert(operator);
            if (lakePredicate != null) {
                lakePredicates.add(lakePredicate);
            }
        }

        if (!lakePredicates.isEmpty()) {
            lakeSource.withFilters(lakePredicates);
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
                                         TvrVersionRange versionRange) {
        Statistics.Builder builder = Statistics.builder();
        for (ColumnRefOperator columnRefOperator : columns.keySet()) {
            builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
        }
        builder.setOutputRowCount(1);
        return builder.build();
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        FlussTable flussTable = (FlussTable) table;
        TablePath identifier = TablePath.of(flussTable.getCatalogDBName(), flussTable.getCatalogTableName());
        List<PartitionInfo> result = new ArrayList<>();
        if (table.isUnPartitioned()) {
            result.add(new Partition(flussTable.getCatalogTableName(), System.currentTimeMillis()));
            return result;
        }
        Map<String, Partition> partitionInfo = this.partitionInfos.get(identifier);
        for (String partitionName : partitionNames) {
            if (partitionInfo == null || partitionInfo.get(partitionName) == null) {
                this.updatePartitionInfo(flussTable.getCatalogDBName(), flussTable.getCatalogTableName());
                partitionInfo = this.partitionInfos.get(identifier);
            }
            if (partitionInfo.get(partitionName) != null) {
                result.add(partitionInfo.get(partitionName));
            } else {
                LOG.warn("Cannot find the fluss partition info: {}", partitionName);
            }
        }
        return result;
    }
}
