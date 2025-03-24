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

package com.starrocks.connector.paimon;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.system.SchemasTable;
import org.apache.paimon.table.system.SnapshotsTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.PartitionPathUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;

public class PaimonMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(PaimonMetadata.class);

    public static final String PAIMON_PARTITION_NULL_VALUE = "null";
    private final Catalog paimonNativeCatalog;
    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private final Map<Identifier, Table> tables = new ConcurrentHashMap<>();
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<PredicateSearchKey, PaimonSplitsInfo> paimonSplits = new ConcurrentHashMap<>();
    private final ConnectorProperties properties;
    private final Map<String, Partition> partitionInfos = new ConcurrentHashMap<>();

    public PaimonMetadata(String catalogName, HdfsEnvironment hdfsEnvironment, Catalog paimonNativeCatalog,
                          ConnectorProperties properties) {
        this.paimonNativeCatalog = paimonNativeCatalog;
        this.hdfsEnvironment = hdfsEnvironment;
        this.catalogName = catalogName;
        this.properties = properties;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.PAIMON;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        return paimonNativeCatalog.listDatabases();
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        try {
            return paimonNativeCatalog.listTables(dbName);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new StarRocksConnectorException("Database %s not exists", dbName);
        }
    }

    private void updatePartitionInfo(String databaseName, String tableName) {
        Identifier identifier = new Identifier(databaseName, tableName);
        org.apache.paimon.table.Table paimonTable;
        RowType dataTableRowType;
        try {
            paimonTable = this.paimonNativeCatalog.getTable(identifier);
            dataTableRowType = paimonTable.rowType();
        } catch (Catalog.TableNotExistException e) {
            throw new StarRocksConnectorException(String.format("Paimon table %s.%s does not exist.", databaseName, tableName));
        }
        List<String> partitionColumnNames = paimonTable.partitionKeys();
        if (partitionColumnNames.isEmpty()) {
            return;
        }

        List<DataType> partitionColumnTypes = new ArrayList<>();
        for (String partitionColumnName : partitionColumnNames) {
            partitionColumnTypes.add(dataTableRowType.getTypeAt(dataTableRowType.getFieldIndex(partitionColumnName)));
        }

        try {
            List<org.apache.paimon.partition.Partition> partitions = paimonNativeCatalog.listPartitions(identifier);
            for (org.apache.paimon.partition.Partition partition : partitions) {
                String partitionPath = PartitionPathUtils.generatePartitionPath(partition.spec(), dataTableRowType);
                String[] partitionValues = Arrays.stream(partitionPath.split("/"))
                        .map(part -> part.split("=")[1])
                        .toArray(String[]::new);
                Partition srPartition = getPartition(partition.recordCount(),
                        partition.fileSizeInBytes(), partition.fileCount(),
                        partitionColumnNames, partitionColumnTypes, partitionValues,
                        Timestamp.fromEpochMillis(partition.lastFileCreationTime()));
                this.partitionInfos.put(srPartition.getPartitionName(), srPartition);
            }
        } catch (Catalog.TableNotExistException e) {
            LOG.error("Failed to update partition info of paimon table {}.{}.", databaseName, tableName, e);
        }
    }

    private Partition getPartition(Long recordCount,
                                   Long fileSizeInBytes,
                                   Long fileCount,
                                   List<String> partitionColumnNames,
                                   List<DataType> partitionColumnTypes,
                                   String[] partitionValues,
                                   Timestamp lastUpdateTime) {
        if (partitionValues.length != partitionColumnNames.size()) {
            String errorMsg = String.format("The length of partitionValues %s is not equal to " +
                    "the partitionColumnNames %s.", partitionValues.length, partitionColumnNames.size());
            throw new IllegalArgumentException(errorMsg);
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partitionValues.length; i++) {
            String column = partitionColumnNames.get(i);
            String value = partitionValues[i].trim();
            if (partitionColumnTypes.get(i) instanceof DateType) {
                value = DateTimeUtils.formatDate(Integer.parseInt(value));
            }
            sb.append(column).append("=").append(value);
            sb.append("/");
        }
        sb.deleteCharAt(sb.length() - 1);
        String partitionName = sb.toString();

        return new Partition(partitionName, convertToSystemDefaultTime(lastUpdateTime),
                recordCount, fileSizeInBytes, fileCount);
    }

    private Long convertToSystemDefaultTime(Timestamp lastUpdateTime) {
        LocalDateTime localDateTime = lastUpdateTime.toLocalDateTime();
        ZoneId zoneId = ZoneId.systemDefault();
        ZonedDateTime zonedDateTime = localDateTime.atZone(zoneId);
        return zonedDateTime.toInstant().toEpochMilli();
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName, ConnectorMetadatRequestContext requestContext) {
        updatePartitionInfo(databaseName, tableName);
        return new ArrayList<>(this.partitionInfos.keySet());
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        if (databases.containsKey(dbName)) {
            return databases.get(dbName);
        }
        try {
            // get database from paimon catalog to see if the database exists
            paimonNativeCatalog.getDatabase(dbName);
            Database db = new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), dbName);
            databases.put(dbName, db);
            return db;
        } catch (Catalog.DatabaseNotExistException e) {
            LOG.error("Paimon database {}.{} does not exist.", catalogName, dbName);
            return null;
        }
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        Identifier identifier = new Identifier(dbName, tblName);
        if (tables.containsKey(identifier)) {
            return tables.get(identifier);
        }
        org.apache.paimon.table.Table paimonNativeTable;
        try {
            paimonNativeTable = this.paimonNativeCatalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            LOG.error("Paimon table {}.{} does not exist.", dbName, tblName, e);
            return null;
        }
        List<DataField> fields = paimonNativeTable.rowType().getFields();
        ArrayList<Column> fullSchema = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            String fieldName = field.name();
            DataType type = field.type();
            Type fieldType = ColumnTypeConverter.fromPaimonType(type);
            Column column = new Column(fieldName, fieldType, true, field.description());
            fullSchema.add(column);
        }
        long createTime = this.getTableCreateTime(dbName, tblName);
        String comment = "";
        if (paimonNativeTable.comment().isPresent()) {
            comment = paimonNativeTable.comment().get();
        }
        PaimonTable table = new PaimonTable(this.catalogName, dbName, tblName, fullSchema, paimonNativeTable, createTime);
        table.setComment(comment);
        tables.put(identifier, table);
        return table;
    }

    @Override
    public boolean tableExists(ConnectContext context, String dbName, String tableName) {
        try {
            paimonNativeCatalog.getTable(Identifier.create(dbName, tableName));
            return true;
        } catch (Catalog.TableNotExistException e) {
            return false;
        }
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        PaimonTable paimonTable = (PaimonTable) table;
        long latestSnapshotId = -1L;
        try {
            if (paimonTable.getNativeTable().latestSnapshotId().isPresent()) {
                latestSnapshotId = paimonTable.getNativeTable().latestSnapshotId().getAsLong();
            }
        } catch (Exception e) {
            // System table does not have snapshotId, ignore it.
            LOG.warn("Cannot get snapshot because {}", e.getMessage());
        }
        PredicateSearchKey filter = PredicateSearchKey.of(paimonTable.getCatalogDBName(),
                paimonTable.getCatalogTableName(), latestSnapshotId, params.getPredicate());
        if (!paimonSplits.containsKey(filter)) {
            ReadBuilder readBuilder = paimonTable.getNativeTable().newReadBuilder();
            int[] projected =
                    params.getFieldNames().stream().mapToInt(name -> (paimonTable.getFieldNames().indexOf(name))).toArray();
            List<Predicate> predicates = extractPredicates(paimonTable, params.getPredicate());
            InnerTableScan scan = (InnerTableScan) readBuilder.withFilter(predicates).withProjection(projected).newScan();
            PaimonMetricRegistry paimonMetricRegistry = new PaimonMetricRegistry();
            List<Split> splits = scan.withMetricsRegistry(paimonMetricRegistry).plan().splits();
            traceScanMetrics(paimonMetricRegistry, splits, table.getCatalogTableName(), predicates);

            PaimonSplitsInfo paimonSplitsInfo = new PaimonSplitsInfo(predicates, splits);
            paimonSplits.put(filter, paimonSplitsInfo);
            List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(
                    PaimonRemoteFileDesc.createPaimonRemoteFileDesc(paimonSplitsInfo));
            remoteFileInfo.setFiles(remoteFileDescs);
        } else {
            List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(
                    PaimonRemoteFileDesc.createPaimonRemoteFileDesc(paimonSplits.get(filter)));
            remoteFileInfo.setFiles(remoteFileDescs);
        }

        return Lists.newArrayList(remoteFileInfo);
    }

    private void traceScanMetrics(PaimonMetricRegistry metricRegistry,
                                  List<Split> splits,
                                  String tableName,
                                  List<Predicate> predicates) {
        // Don't need scan metrics when selecting system table, in which metric group is null.
        if (metricRegistry.getMetricGroup() == null) {
            return;
        }
        String prefix = "Paimon.plan.";

        if (paimonNativeCatalog instanceof CachingCatalog) {
            CachingCatalog.CacheSizes cacheSizes = ((CachingCatalog) paimonNativeCatalog).estimatedCacheSizes();
            Tracers.record(EXTERNAL, prefix + "total.cachedDatabaseNumInCatalog",
                    String.valueOf(cacheSizes.databaseCacheSize()));
            Tracers.record(EXTERNAL, prefix + "total.cachedTableNumInCatalog",
                    String.valueOf(cacheSizes.tableCacheSize()));
            Tracers.record(EXTERNAL, prefix + "total.cachedManifestNumInCatalog",
                    String.valueOf(cacheSizes.manifestCacheSize()));
            Tracers.record(EXTERNAL, prefix + "total.cachedManifestBytesInCatalog",
                    cacheSizes.manifestCacheBytes() + " B");
            Tracers.record(EXTERNAL, prefix + "total.cachedPartitionNumInCatalog",
                    String.valueOf(cacheSizes.partitionCacheSize()));
        }

        for (int i = 0; i < predicates.size(); i++) {
            Tracers.record(EXTERNAL, prefix + tableName + ".filter." + i, predicates.get(i).toString());
        }

        Map<String, Metric> metrics = metricRegistry.getMetrics();
        long manifestFileReadTime = (long) ((Gauge<?>) metrics.get(ScanMetrics.LAST_SCAN_DURATION)).getValue();
        long scannedManifestFileNum = (long) ((Gauge<?>) metrics.get(ScanMetrics.LAST_SCANNED_MANIFESTS)).getValue();
        long skippedDataFilesNum = (long) ((Gauge<?>) metrics.get(ScanMetrics.LAST_SCAN_SKIPPED_TABLE_FILES)).getValue();
        long resultedDataFilesNum = (long) ((Gauge<?>) metrics.get(ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES)).getValue();
        long manifestNumReadFromCache = (long) ((Gauge<?>) metrics.get(ScanMetrics.MANIFEST_HIT_CACHE)).getValue();
        long manifestNumReadFromRemote = (long) ((Gauge<?>) metrics.get(ScanMetrics.MANIFEST_MISSED_CACHE)).getValue();

        Tracers.record(EXTERNAL, prefix + tableName + "." + "manifestFileReadTime", manifestFileReadTime + "ms");
        Tracers.record(EXTERNAL, prefix + tableName + "." + "scannedManifestFileNum", String.valueOf(scannedManifestFileNum));
        Tracers.record(EXTERNAL, prefix + tableName + "." + "skippedDataFilesNum", String.valueOf(skippedDataFilesNum));
        Tracers.record(EXTERNAL, prefix + tableName + "." + "resultedDataFilesNum", String.valueOf(resultedDataFilesNum));
        Tracers.record(EXTERNAL, prefix + tableName + "." + "manifestNumReadFromCache",
                String.valueOf(manifestNumReadFromCache));
        Tracers.record(EXTERNAL, prefix + tableName + "." + "manifestNumReadFromRemote",
                String.valueOf(manifestNumReadFromRemote));
        Tracers.record(EXTERNAL, prefix + "total.resultSplitsNum", String.valueOf(splits.size()));

        AtomicLong resultedTableFilesSize = new AtomicLong(0);
        for (Split split : splits) {
            List<DataFileMeta> dataFileMetas = ((DataSplit) split).dataFiles();
            dataFileMetas.forEach(dataFileMeta -> resultedTableFilesSize.addAndGet(dataFileMeta.fileSize()));
        }
        Tracers.record(EXTERNAL, prefix + tableName + "." + "resultedDataFilesSize", resultedTableFilesSize.get() + " B");
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit,
                                         TableVersionRange versionRange) {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "GetPaimonTableStatistics")) {
            if (!properties.enableGetTableStatsFromExternalMetadata()) {
                return StatisticsUtils.buildDefaultStatistics(columns.keySet());
            }

            Statistics.Builder builder = Statistics.builder();
            if (!session.getSessionVariable().enablePaimonColumnStatistics()) {
                return defaultStatistics(columns, table, predicate, limit);
            }
            org.apache.paimon.table.Table nativeTable = ((PaimonTable) table).getNativeTable();
            Optional<org.apache.paimon.stats.Statistics> statistics = nativeTable.statistics();
            if (!statistics.isPresent() || statistics.get().colStats() == null
                    || !statistics.get().mergedRecordCount().isPresent()) {
                return defaultStatistics(columns, table, predicate, limit);
            }
            long rowCount = statistics.get().mergedRecordCount().getAsLong();
            builder.setOutputRowCount(rowCount);
            Map<String, ColStats<?>> colStatsMap = statistics.get().colStats();
            for (ColumnRefOperator column : columns.keySet()) {
                builder.addColumnStatistic(column, buildColumnStatistic(columns.get(column), colStatsMap, rowCount));
            }
            return builder.build();
        }
    }

    private Statistics defaultStatistics(Map<ColumnRefOperator, Column> columns, Table table, ScalarOperator predicate,
                                         long limit) {
        Statistics.Builder builder = Statistics.builder();
        for (ColumnRefOperator columnRefOperator : columns.keySet()) {
            builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
        }
        List<String> fieldNames = columns.keySet().stream().map(ColumnRefOperator::getName).collect(Collectors.toList());
        GetRemoteFilesParams params =
                GetRemoteFilesParams.newBuilder().setPredicate(predicate).setFieldNames(fieldNames).setLimit(limit).build();
        List<RemoteFileInfo> fileInfos = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFiles(table, params);
        PaimonRemoteFileDesc remoteFileDesc = (PaimonRemoteFileDesc) fileInfos.get(0).getFiles().get(0);
        List<Split> splits = remoteFileDesc.getPaimonSplitsInfo().getPaimonSplits();
        long rowCount = getRowCount(splits);
        if (rowCount == 0) {
            builder.setOutputRowCount(1);
        } else {
            builder.setOutputRowCount(rowCount);
        }
        return builder.build();

    }


    private ColumnStatistic buildColumnStatistic(Column column, Map<String, ColStats<?>> colStatsMap,
                                                 long rowCount) {
        ColumnStatistic columnStatistic = null;
        for (Map.Entry<String, ColStats<?>> colStatsEntry : colStatsMap.entrySet()) {
            if (!colStatsEntry.getKey().equalsIgnoreCase(column.getName())) {
                continue;
            }
            ColumnStatistic.Builder builder = ColumnStatistic.builder();
            ColStats<?> colStats = colStatsEntry.getValue();
            Optional<? extends Comparable<?>> min = colStats.min();
            if (min.isPresent() && min.get() != null) {
                if (column.getType().isBoolean()) {
                    builder.setMinValue((Boolean) min.get() ? 1 : 0);
                } else if (column.getType().isDatetime()) {
                    builder.setMinValue(getLongFromDateTime(((Timestamp) min.get()).toLocalDateTime()));
                } else {
                    builder.setMinValue(Double.parseDouble(min.get().toString()));
                }
            }

            Optional<? extends Comparable<?>> max = colStats.max();
            if (max.isPresent() && max.get() != null) {
                if (column.getType().isBoolean()) {
                    builder.setMaxValue((Boolean) max.get() ? 1 : 0);
                } else if (column.getType().isDatetime()) {
                    builder.setMaxValue(getLongFromDateTime(((Timestamp) max.get()).toLocalDateTime()));
                } else if (!column.getType().isBoolean()) {
                    builder.setMaxValue(Double.parseDouble(max.get().toString()));
                }
            }

            if (colStats.nullCount().isPresent()) {
                builder.setNullsFraction(colStats.nullCount().getAsLong() * 1.0 / Math.max(rowCount, 1));
            } else {
                builder.setNullsFraction(0);
            }

            builder.setAverageRowSize(colStats.nullCount().isPresent() ? colStats.nullCount().getAsLong() : 1);

            if (colStats.distinctCount().isPresent()) {
                builder.setDistinctValuesCount(colStats.distinctCount().getAsLong());
                builder.setType(ColumnStatistic.StatisticType.ESTIMATE);
            } else {
                builder.setDistinctValuesCount(1);
                builder.setType(ColumnStatistic.StatisticType.UNKNOWN);
            }
            columnStatistic = builder.build();
        }

        if (columnStatistic == null) {
            columnStatistic = ColumnStatistic.unknown();
        }
        return columnStatistic;
    }

    public static long getRowCount(List<? extends Split> splits) {
        long rowCount = 0;
        for (Split split : splits) {
            rowCount += split.rowCount();
        }
        return rowCount;
    }

    private List<Predicate> extractPredicates(PaimonTable paimonTable, ScalarOperator predicate) {
        List<ScalarOperator> scalarOperators = Utils.extractConjuncts(predicate);
        List<Predicate> predicates = new ArrayList<>(scalarOperators.size());

        PaimonPredicateConverter converter = new PaimonPredicateConverter(paimonTable.getNativeTable().rowType());
        for (ScalarOperator operator : scalarOperators) {
            Predicate filter = converter.convert(operator);
            if (filter != null) {
                predicates.add(filter);
            }
        }
        return predicates;
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return hdfsEnvironment.getCloudConfiguration();
    }

    public long getTableCreateTime(String dbName, String tblName) {
        Identifier schemaTableIdentifier = new Identifier(dbName, String.format("%s%s", tblName, "$schemas"));
        RecordReaderIterator<InternalRow> iterator = null;
        try {
            SchemasTable table = (SchemasTable) paimonNativeCatalog.getTable(schemaTableIdentifier);
            RowType rowType = table.rowType();
            if (!rowType.getFieldNames().contains("update_time")) {
                return 0;
            }
            DataType updateTimeType = rowType.getTypeAt(rowType.getFieldIndex("update_time"));
            int[] projected = new int[] {0, 6};
            PredicateBuilder predicateBuilder = new PredicateBuilder(rowType);
            Predicate equal = predicateBuilder.equal(predicateBuilder.indexOf("schema_id"), 0L);
            RecordReader<InternalRow> recordReader = table.newReadBuilder().withProjection(projected)
                    .withFilter(equal).newRead().createReader(table.newScan().plan());
            iterator = new RecordReaderIterator<>(recordReader);
            while (iterator.hasNext()) {
                InternalRow rowData = iterator.next();
                Long schemaIdValue = rowData.getLong(0);
                Timestamp updateTime = rowData.getTimestamp(1, DataTypeChecks.getPrecision(updateTimeType));
                if (schemaIdValue == 0) {
                    return updateTime.getMillisecond();
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to get update_time of paimon table {}.{}.", dbName, tblName, e);
        } finally {
            if (iterator != null) {
                try {
                    iterator.close();
                } catch (Exception e) {
                    LOG.error("Failed to get update_time of paimon table {}.{}.", dbName, tblName, e);
                }
            }
        }
        return 0;
    }

    public long getTableUpdateTime(String dbName, String tblName) {
        Identifier snapshotsTableIdentifier = new Identifier(dbName, String.format("%s%s", tblName, "$snapshots"));
        RecordReaderIterator<InternalRow> iterator = null;
        long lastCommitTime = -1;
        try {
            SnapshotsTable table = (SnapshotsTable) paimonNativeCatalog.getTable(snapshotsTableIdentifier);
            RowType rowType = table.rowType();
            if (!rowType.getFieldNames().contains("commit_time")) {
                return System.currentTimeMillis();
            }
            DataType commitTimeType = rowType.getTypeAt(rowType.getFieldIndex("commit_time"));
            int[] projected = new int[] {5};
            RecordReader<InternalRow> recordReader = table.newReadBuilder().withProjection(projected)
                    .newRead().createReader(table.newScan().plan());
            iterator = new RecordReaderIterator<>(recordReader);
            while (iterator.hasNext()) {
                InternalRow rowData = iterator.next();
                Timestamp commitTime = rowData.getTimestamp(0, DataTypeChecks.getPrecision(commitTimeType));
                if (convertToSystemDefaultTime(commitTime) > lastCommitTime) {
                    lastCommitTime = convertToSystemDefaultTime(commitTime);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to get commit_time of paimon table {}.{}.", dbName, tblName, e);
        } finally {
            if (iterator != null) {
                try {
                    iterator.close();
                } catch (Exception e) {
                    LOG.error("Failed to get commit_time of paimon table {}.{}.", dbName, tblName, e);
                }
            }
        }
        if (lastCommitTime == -1) {
            lastCommitTime = System.currentTimeMillis();
        }
        return lastCommitTime;
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        PaimonTable paimonTable = (PaimonTable) table;
        List<PartitionInfo> result = new ArrayList<>();
        if (table.isUnPartitioned()) {

            result.add(new Partition(paimonTable.getCatalogTableName(),
                    this.getTableUpdateTime(paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName()), null,
                    null, null));
            return result;
        }
        for (String partitionName : partitionNames) {
            if (this.partitionInfos.get(partitionName) == null) {
                this.updatePartitionInfo(paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName());
            }
            if (this.partitionInfos.get(partitionName) != null) {
                result.add(this.partitionInfos.get(partitionName));
            } else {
                LOG.warn("Cannot find the paimon partition info: {}", partitionName);
            }
        }
        return result;
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        String tableName = table.getCatalogTableName();
        Identifier identifier = new Identifier(srDbName, tableName);
        paimonNativeCatalog.invalidateTable(identifier);
        try {
            ((PaimonTable) table).setPaimonNativeTable(paimonNativeCatalog.getTable(identifier));
            if (partitionNames != null && !partitionNames.isEmpty()) {
                // todo: paimon does not support to refresh an exact partition
                this.refreshPartitionInfo(identifier);
            } else {
                this.refreshPartitionInfo(identifier);
            }
            // Preheat manifest files, disabled by default
            if (Config.enable_paimon_refresh_manifest_files) {
                if (partitionNames == null || partitionNames.isEmpty()) {
                    ((PaimonTable) table).getNativeTable().newReadBuilder().newScan().plan();
                } else {
                    List<String> partitionColumnNames = table.getPartitionColumnNames();
                    Map<String, String> partitionSpec = new HashMap<>();
                    for (String partitionName : partitionNames) {
                        partitionSpec.put(String.join(",", partitionColumnNames), partitionName);
                    }
                    ((PaimonTable) table).getNativeTable().newReadBuilder()
                            .withPartitionFilter(partitionSpec).newScan().plan();
                }
            }
            tables.put(identifier, table);
        } catch (Exception e) {
            LOG.error("Failed to refresh table {}.{}.{}.", catalogName, srDbName, tableName, e);
        }
    }

    private void refreshPartitionInfo(Identifier identifier) {
        if (paimonNativeCatalog instanceof CachingCatalog) {
            try {
                paimonNativeCatalog.invalidateTable(identifier);
                ((CachingCatalog) paimonNativeCatalog).refreshPartitions(identifier);
            } catch (Catalog.TableNotExistException e) {
                throw new RuntimeException(e);
            }
        } else {
            LOG.warn("Current catalog {} does not support cache.", catalogName);
        }
    }
}
