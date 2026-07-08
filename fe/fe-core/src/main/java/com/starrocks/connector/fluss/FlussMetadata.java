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
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.PartitionInfo;
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
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.connector.PartitionUtil.toHivePartitionName;
import static org.apache.fluss.flink.utils.CatalogExceptionUtils.isTableNotExist;
import static org.apache.fluss.flink.utils.CatalogExceptionUtils.isTableNotPartitioned;
import static org.apache.fluss.flink.utils.LakeSourceUtils.createLakeSource;

public class FlussMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(FlussMetadata.class);

    private static final String LAKE_TABLE_SPLITTER = "$lake";
    public static final String RT_TABLE_SPLITTER = "$rt";
    private static final int MAX_LOG_PARTITION_NAME_LENGTH = 128;

    private final Connection connection;
    private final Admin admin;

    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private final Map<TablePath, Table> tables = new ConcurrentHashMap<>();
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<TablePath, Map<String, Partition>> partitionInfos = new ConcurrentHashMap<>();
    private final Map<TablePath, Map<String, org.apache.fluss.metadata.PartitionInfo>> flussPartitionInfos =
            new ConcurrentHashMap<>();
    // Catalog-level Fluss/lake options copied from CREATE EXTERNAL CATALOG.
    private final Configuration catalogConf;

    public FlussMetadata(String catalogName, HdfsEnvironment hdfsEnvironment, Connection connection, Admin admin,
                         Configuration catalogConf) {
        this.catalogName = catalogName;
        this.hdfsEnvironment = hdfsEnvironment;
        this.connection = connection;
        this.admin = admin;
        this.catalogConf = catalogConf;
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

    private Map<String, org.apache.fluss.metadata.PartitionInfo> loadPartitionInfo(String databaseName, String tableName) {
        TablePath identifier = TablePath.of(databaseName, tableName);
        try {
            List<org.apache.fluss.metadata.PartitionInfo> flussPartitions =
                    admin.listPartitionInfos(identifier).get();
            Map<String, org.apache.fluss.metadata.PartitionInfo> flussPartitionMap = new LinkedHashMap<>();
            Map<String, Partition> srPartitionMap = new LinkedHashMap<>();
            for (org.apache.fluss.metadata.PartitionInfo partitionInfo : flussPartitions) {
                String qualifiedName = partitionInfo.getResolvedPartitionSpec().getPartitionQualifiedName();
                Partition srPartition = new Partition(qualifiedName, System.currentTimeMillis());
                flussPartitionMap.put(qualifiedName, partitionInfo);
                srPartitionMap.put(qualifiedName, srPartition);
            }
            this.partitionInfos.put(identifier, srPartitionMap);
            return flussPartitionMap;
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (isTableNotExist(t)) {
                LOG.error("Failed to list Fluss partition names {}.{}.{} for TableNotExist.",
                        catalogName, databaseName, tableName, e);
                throw new StarRocksConnectorException(e.getMessage());
            } else if (isTableNotPartitioned(t)) {
                LOG.debug("Skip listing Fluss partition names for unpartitioned table {}.{}.{}.",
                        catalogName, databaseName, tableName);
                this.partitionInfos.put(identifier, new LinkedHashMap<>());
                return new LinkedHashMap<>();
            } else {
                LOG.error("Failed to list Fluss partition names {}.{}.{}.",
                        catalogName, databaseName, tableName, e);
                throw new StarRocksConnectorException(e.getMessage());
            }
        }
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName,
                                           ConnectorMetadataRequestContext requestContext) {
        TablePath identifier = TablePath.of(databaseName, tableName);
        Map<String, org.apache.fluss.metadata.PartitionInfo> partitionInfo =
                flussPartitionInfos.computeIfAbsent(identifier, ignored -> loadPartitionInfo(databaseName, tableName));
        return new ArrayList<>(partitionInfo.keySet());
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        if (this.databases.containsKey(dbName)) {
            return this.databases.get(dbName);
        }
        try {
            this.admin.getDatabaseInfo(dbName);
            Database db = new Database(CONNECTOR_ID_GENERATOR.getNextId().asLong(), dbName);
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
        if (tblName.contains(LAKE_TABLE_SPLITTER)) {
            realTblName = tblName.split("\\" + LAKE_TABLE_SPLITTER)[0];
        }
        if (tblName.contains(RT_TABLE_SPLITTER)) {
            realTblName = tblName.split("\\" + RT_TABLE_SPLITTER)[0];
        }
        TablePath flussIdentifier = TablePath.of(dbName, realTblName);

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
            FlussTable table = new FlussTable(catalogName, dbName, realTblName, fullSchema,
                    connection.getTable(flussIdentifier), catalogConf);
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
        Map<String, org.apache.fluss.metadata.PartitionInfo> partitionInfo =
                flussPartitionInfos.computeIfAbsent(identifier, ignored -> loadPartitionInfo(
                        flussTable.getCatalogDBName(), flussTable.getCatalogTableName()));
        return new ArrayList<>(partitionInfo.values());
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        FlussTable flussTable = (FlussTable) table;
        TablePath identifier = TablePath.of(flussTable.getCatalogDBName(), flussTable.getCatalogTableName());
        TableInfo tableInfo = flussTable.getTableInfo();

        OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever =
                new BucketOffsetsRetrieverImpl(admin, identifier);

        List<Predicate> lakePredicates = Lists.newArrayList();
        LakeSource<LakeSplit> lakeSource =
                createLakeSource(flussTable.getTableInfo().getTablePath(), flussTable.buildRuntimeConf().toMap());
        if (lakeSource != null) {
            try {
                lakePredicates = convertLakePredicates(flussTable, params.getPredicate());
                if (!lakePredicates.isEmpty()) {
                    lakeSource.withFilters(lakePredicates);
                }
            } catch (Exception e) {
                LOG.warn("Failed to push down predicates to lake source for table {}, " +
                        "falling back to scan without filter pushdown", identifier, e);
                lakePredicates = Lists.newArrayList();
            }
        }

        List<PartitionKey> selectedPartitionKeys = params.getPartitionKeys();
        // Null means the caller did not provide an FE pruning result, so partitioned tables fall back to all
        // Fluss partitions. An empty list means FE pruning has run and selected zero partitions; return empty splits
        // without asking Fluss for a lake snapshot.
        Set<String> selectedQualifiedPartitionNames = null;
        Set<String> selectedFlussPartitionNames = null;
        Supplier<Set<org.apache.fluss.metadata.PartitionInfo>> listPartitionSupplier;
        boolean selectedNoPartitions = false;
        if (flussTable.isUnPartitioned()) {
            listPartitionSupplier = LinkedHashSet::new;
        } else if (selectedPartitionKeys == null) {
            listPartitionSupplier = () -> new LinkedHashSet<>(listFlussPartitions(table));
        } else if (selectedPartitionKeys.isEmpty()) {
            selectedNoPartitions = true;
            selectedQualifiedPartitionNames = new LinkedHashSet<>();
            selectedFlussPartitionNames = new LinkedHashSet<>();
            listPartitionSupplier = LinkedHashSet::new;
        } else {
            // Fluss ResolvedPartitionSpec#getPartitionQualifiedName uses the same key=value/key2=value2 shape
            // as Hive partition names for normal partition values; raw value-only partition names are logged below.
            selectedQualifiedPartitionNames = selectedPartitionKeys.stream()
                    .filter(Objects::nonNull)
                    .map(partitionKey -> toHivePartitionName(flussTable.getPartitionColumnNames(), partitionKey))
                    .collect(Collectors.toSet());
            List<org.apache.fluss.metadata.PartitionInfo> allPartitions = listFlussPartitions(table);
            Set<String> selectedNames = selectedQualifiedPartitionNames;
            Set<org.apache.fluss.metadata.PartitionInfo> selectedPartitions = allPartitions.stream()
                    .filter(p -> selectedNames.contains(
                            p.getResolvedPartitionSpec().getPartitionQualifiedName()))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            selectedFlussPartitionNames = selectedPartitions.stream()
                    .map(org.apache.fluss.metadata.PartitionInfo::getPartitionName)
                    .collect(Collectors.toSet());
            listPartitionSupplier = () -> selectedPartitions;
        }
        LOG.debug("Fluss remote file partition params table={}.{}.{}, inputPartitionKeys={}, " +
                        "selectedQualifiedPartitions={}, selectedFlussPartitions={}, predicate={}, " +
                        "fieldNames={}, limit={}",
                catalogName, flussTable.getCatalogDBName(), flussTable.getCatalogTableName(),
                selectedPartitionKeys, selectedQualifiedPartitionNames, selectedFlussPartitionNames,
                params.getPredicate(), params.getFieldNames(), params.getLimit());
        List<SourceSplitBase> splits = new ArrayList<>();
        if (!selectedNoPartitions) {
            LakeSplitGenerator lakeSplitGenerator = new LakeSplitGenerator(
                    tableInfo, admin,
                    lakeSource, bucketOffsetsRetriever,
                    new LatestOffsetsInitializer(), tableInfo.getNumBuckets(), listPartitionSupplier);
            try {
                splits = lakeSplitGenerator.generateHybridLakeFlussSplits();
            } catch (Exception e) {
                LOG.error("Failed to get Fluss splits for table {}.{}.{}.",
                        catalogName, flussTable.getCatalogDBName(), flussTable.getCatalogTableName(), e);
                throw new StarRocksConnectorException("Failed to get Fluss splits for table %s.%s.%s: %s",
                        catalogName, flussTable.getCatalogDBName(), flussTable.getCatalogTableName(), e.getMessage());
            }
            if (splits == null) {
                // Fluss returns null when no readable lake snapshot exists. Do not turn that into an empty result:
                // empty splits are reserved for predicates pruned to zero partitions.
                throw new StarRocksConnectorException("No readable Fluss lake snapshot exists for table %s.%s.%s",
                        catalogName, flussTable.getCatalogDBName(), flussTable.getCatalogTableName());
            }
            LOG.debug("Fluss split generation result table={}.{}.{}, splitCount={}, splitPartitions={}",
                    catalogName, flussTable.getCatalogDBName(), flussTable.getCatalogTableName(),
                    splits.size(), summarizeSplitPartitions(splits));
        }
        if (flussTable.getTableNamePrefix().equals(LAKE_TABLE_SPLITTER)) {
            // Flink supports $lake reads on primary-key tables via LakeSnapshotAndFlussLogSplit.
            splits = splits.stream().filter(SourceSplitBase::isLakeSplit)
                    .collect(Collectors.toList());
        }
        if (flussTable.getTableNamePrefix().equals(RT_TABLE_SPLITTER)) {
            splits = splits.stream().filter(sp -> sp instanceof LogSplit)
                    .collect(Collectors.toList());
        }

        FlussSplitsInfo flussSplitsInfo = new FlussSplitsInfo(lakePredicates, splits);
        List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(
                FlussRemoteFileDesc.createFlussRemoteFileDesc(flussSplitsInfo));
        remoteFileInfo.setFiles(remoteFileDescs);
        return Lists.newArrayList(remoteFileInfo);
    }

    private Map<String, Long> summarizeSplitPartitions(List<SourceSplitBase> splits) {
        return splits.stream().collect(Collectors.groupingBy(
                split -> formatPartitionNameForLog(split.getPartitionName()), Collectors.counting()));
    }

    private String formatPartitionNameForLog(String partitionName) {
        if (partitionName == null) {
            return "null";
        }

        StringBuilder builder = new StringBuilder();
        boolean escaped = false;
        int index = 0;
        while (index < partitionName.length() && builder.length() < MAX_LOG_PARTITION_NAME_LENGTH) {
            char ch = partitionName.charAt(index++);
            if (ch >= 0x20 && ch <= 0x7e) {
                builder.append(ch);
            } else {
                builder.append("\\u");
                String hex = Integer.toHexString(ch);
                for (int i = hex.length(); i < 4; i++) {
                    builder.append('0');
                }
                builder.append(hex);
                escaped = true;
            }
        }
        if (index < partitionName.length()) {
            builder.append("...");
        }
        return escaped ? "escaped:" + builder : builder.toString();
    }

    private List<Predicate> convertLakePredicates(FlussTable flussTable, ScalarOperator predicate) {
        if (predicate == null) {
            return Lists.newArrayList();
        }
        RowType flussRowType = flussTable.getTableInfo().getRowType();
        ZoneId sessionZoneId = ZoneId.of(TimeUtils.getSessionTimeZone());
        FlussPredicateConverter lakeConverter = new FlussPredicateConverter(flussRowType, sessionZoneId);
        List<Predicate> lakePredicates = new ArrayList<>();

        List<ScalarOperator> scalarOperators = Utils.extractConjuncts(predicate);
        for (ScalarOperator operator : scalarOperators) {
            Predicate lakePredicate = lakeConverter.convert(operator);
            if (lakePredicate != null) {
                lakePredicates.add(lakePredicate);
            }
        }
        return lakePredicates;
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
        // TODO(Fluss): read row counts from Fluss/lake metadata when connector statistics are available.
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
        if (partitionInfo == null) {
            flussPartitionInfos.computeIfAbsent(identifier, ignored -> loadPartitionInfo(
                    flussTable.getCatalogDBName(), flussTable.getCatalogTableName()));
            partitionInfo = this.partitionInfos.get(identifier);
        }
        for (String partitionName : partitionNames) {
            if (partitionInfo != null && partitionInfo.get(partitionName) != null) {
                result.add(partitionInfo.get(partitionName));
            } else {
                LOG.warn("Cannot find the fluss partition info: {}", partitionName);
            }
        }
        return result;
    }
}
