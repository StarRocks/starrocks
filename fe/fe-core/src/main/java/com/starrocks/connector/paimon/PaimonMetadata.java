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

import com.aliyun.datalake.common.DlfConfig;
import com.aliyun.datalake.common.DlfContext;
import com.aliyun.datalake.common.DlfDataToken;
import com.aliyun.datalake.common.DlfResource;
import com.aliyun.datalake.common.DlfTokenClient;
import com.aliyun.datalake.common.impl.Base64Util;
import com.aliyun.datalake.external.com.fasterxml.jackson.databind.ObjectMapper;
import com.aliyun.datalake.paimon.catalog.DlfPaimonCatalog;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DlfUtil;
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
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.TPaimonCommitMessage;
import com.starrocks.thrift.TSinkCommitInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.StringUtils;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.aliyun.datalake.core.constant.DataLakeConfig.CATALOG_ID;
import static com.aliyun.datalake.core.constant.DataLakeConfig.DATA_CREDENTIAL_PROVIDER_URL;
import static com.aliyun.datalake.core.constant.DataLakeConfig.DLF_ENDPOINT;
import static com.aliyun.datalake.core.constant.DataLakeConfig.DLF_REGION;
import static com.aliyun.datalake.core.constant.DataLakeConfig.META_CREDENTIAL_PROVIDER;
import static com.aliyun.datalake.core.constant.DataLakeConfig.META_CREDENTIAL_PROVIDER_URL;
import static com.starrocks.catalog.KeysType.PRIMARY_KEYS;
import static com.starrocks.catalog.PaimonTable.FILE_FORMAT;
import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static com.starrocks.connector.ColumnTypeConverter.toPaimonSchema;
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
    private final Map<Identifier, Map<String, Partition>> partitionInfos = new ConcurrentHashMap<>();

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
    public void createDb(String dbName, Map<String, String> properties) throws DdlException {
        try {
            paimonNativeCatalog.createDatabase(dbName, false, properties);
        } catch (Exception e) {
            LOG.error("Failed to create Paimon database {}.{}.", catalogName, dbName, e);
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public void dropDb(String dbName, boolean isForceDrop) throws DdlException {
        try {
            paimonNativeCatalog.dropDatabase(dbName, false, isForceDrop);
        } catch (Exception e) {
            LOG.error("Failed to drop Paimon database {}.{}.", catalogName, dbName, e);
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        try {
            return paimonNativeCatalog.listTables(dbName);
        } catch (Exception e) {
            LOG.error("Failed to list Paimon tables {}.{}.", catalogName, dbName, e);
            throw new StarRocksConnectorException(e.getMessage());
        }
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        refreshDlfDataToken(dbName, "");

        Schema.Builder schemaBuilder = toPaimonSchema(stmt.getColumns());

        KeysDesc keysDesc = stmt.getKeysDesc();
        if (null != keysDesc) {
            if (PRIMARY_KEYS != keysDesc.getKeysType()) {
                throw new DdlException("Paimon table does not support key type " + keysDesc.getKeysType().name());
            }
            schemaBuilder.primaryKey(keysDesc.getKeysColumnNames());
        }

        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        List<String> partitionColNames = partitionDesc == null ? Lists.newArrayList() :
                ((ListPartitionDesc) partitionDesc).getPartitionColNames();
        schemaBuilder.partitionKeys(partitionColNames);

        Map<String, String> properties = stmt.getProperties() == null ? new HashMap<>() : stmt.getProperties();
        properties.putIfAbsent(FILE_FORMAT, "parquet");
        schemaBuilder.options(properties);

        schemaBuilder.comment(stmt.getComment());

        Schema schema = schemaBuilder.build();

        try {
            paimonNativeCatalog.createTable(new Identifier(dbName, tableName), schema, false);
        } catch (Exception e) {
            LOG.error("Failed to create Paimon table {}.{}.{}.", catalogName, dbName, tableName, e);
            throw new DdlException(e.getMessage());
        }
        return true;
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        try {
            paimonNativeCatalog.dropTable(new Identifier(dbName, tableName), false);
        } catch (Exception e) {
            LOG.error("Failed to drop Paimon table {}.{}.{}.", catalogName, dbName, tableName, e);
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException {
        String partitionName = clause.getPartitionName();
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put("dummy", partitionName);
        try {
            paimonNativeCatalog.dropPartitions(new Identifier(db.getOriginName(), table.getName()), List.of(partitionMap));
        } catch (Exception e) {
            LOG.error("Failed to drop Paimon table partition {}.{}.{}.", catalogName, db.getOriginName(), table.getName(), e);
            throw new DdlException(e.getMessage());
        }
    }

    private void updatePartitionInfo(String databaseName, String tableName) {
        Identifier identifier = new Identifier(databaseName, tableName);
        org.apache.paimon.table.Table paimonTable;
        RowType dataTableRowType;
        if (!this.partitionInfos.containsKey(identifier)) {
            this.partitionInfos.put(identifier, new ConcurrentHashMap<>());
        }
        try {
            paimonTable = this.paimonNativeCatalog.getTable(identifier);
            dataTableRowType = paimonTable.rowType();
        } catch (Exception e) {
            LOG.error("Failed to get Paimon table {}.{}.{}.", catalogName, databaseName, tableName, e);
            throw new StarRocksConnectorException(e.getMessage());
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
                this.partitionInfos.get(identifier).put(srPartition.getPartitionName(), srPartition);
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
                if (!partitionKey.nullPartitionValueList().contains(value) && StringUtils.isNumeric(value)) {
                    value = DateTimeUtils.formatDate(Integer.parseInt(value));
                }
            }
            sb.append(column).append("=").append(value);
            sb.append("/");
        }
        sb.deleteCharAt(sb.length() - 1);
        String partitionName = sb.toString();

        return new Partition(partitionName, convertToSystemDefaultTime(lastUpdateTime),
                recordCount, fileSizeInBytes, fileCount);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName, ConnectorMetadatRequestContext requestContext) {
        Identifier identifier = new Identifier(databaseName, tableName);
        updatePartitionInfo(databaseName, tableName);
        if (this.partitionInfos.get(identifier) == null) {
            return Lists.newArrayList();
        }
        return new ArrayList<>(this.partitionInfos.get(identifier).keySet());
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
        } catch (Exception e) {
            LOG.error("Failed to get Paimon database {}.{}.", catalogName, dbName, e);
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
            refreshDlfDataToken(dbName, tblName);
            paimonNativeTable = this.paimonNativeCatalog.getTable(identifier);
        } catch (Exception e) {
            LOG.error("Failed to get Paimon table {}.{}.{}.", catalogName, dbName, tblName, e);
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
        String comment = "";
        if (paimonNativeTable.comment().isPresent()) {
            comment = paimonNativeTable.comment().get();
        }
        PaimonTable table = new PaimonTable(this.catalogName, dbName, tblName, fullSchema, paimonNativeTable);
        table.setComment(comment);
        tables.put(identifier, table);
        return table;
    }

    @Override
    public boolean tableExists(ConnectContext context, String dbName, String tableName) {
        try {
            paimonNativeCatalog.getTable(Identifier.create(dbName, tableName));
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to get Paimon table {}.{}.{}.", catalogName, dbName, tableName, e);
            return false;
        }
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        PaimonTable paimonTable = (PaimonTable) table;
        refreshDlfDataToken(paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName());
        long latestSnapshotId = -1L;
        try {
            if (paimonTable.getNativeTable().latestSnapshot().isPresent()) {
                latestSnapshotId = paimonTable.getNativeTable().latestSnapshot().get().id();
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
            boolean pruneManifestsByLimit = params.getLimit() != -1 && params.getLimit() < Integer.MAX_VALUE
                    && onlyHasPartitionPredicate(table, params.getPredicate());
            readBuilder = readBuilder.withFilter(predicates).withProjection(projected);
            if (pruneManifestsByLimit) {
                readBuilder = readBuilder.withLimit((int) params.getLimit());
            }
            InnerTableScan scan = (InnerTableScan) readBuilder.newScan();
            PaimonMetricRegistry paimonMetricRegistry = new PaimonMetricRegistry();
            List<Split> splits = scan.withMetricRegistry(paimonMetricRegistry).plan().splits();
            traceScanMetrics(paimonMetricRegistry, splits, ((PaimonTable) table).getTableName(), predicates,
                    String.valueOf(Objects.hash(predicate)));

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
                                  List<Predicate> predicates,
                                  String predicateHash) {
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

        String tableIdentifier = tableName + "-" + predicateHash;
        for (int i = 0; i < predicates.size(); i++) {
            Tracers.record(EXTERNAL, prefix + tableIdentifier + ".filter." + i, predicates.get(i).toString());
        }

        Map<String, Metric> metrics = metricRegistry.getMetrics();
        long manifestFileReadTime = (long) ((Gauge<?>) metrics.get(ScanMetrics.LAST_SCAN_DURATION)).getValue();
        long scannedManifestFileNum = (long) ((Gauge<?>) metrics.get(ScanMetrics.LAST_SCANNED_MANIFESTS)).getValue();
        long skippedDataFilesNum = (long) ((Gauge<?>) metrics.get(ScanMetrics.LAST_SCAN_SKIPPED_TABLE_FILES)).getValue();
        long resultedDataFilesNum = (long) ((Gauge<?>) metrics.get(ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES)).getValue();
        long manifestNumReadFromCache = (long) ((Gauge<?>) metrics.get(ScanMetrics.MANIFEST_HIT_CACHE)).getValue();
        long manifestNumReadFromRemote = (long) ((Gauge<?>) metrics.get(ScanMetrics.MANIFEST_MISSED_CACHE)).getValue();

        Tracers.record(EXTERNAL, prefix + tableIdentifier + "." + "manifestFileReadTime", manifestFileReadTime + "ms");
        Tracers.record(EXTERNAL, prefix + tableIdentifier + "." + "scannedManifestFileNum",
                String.valueOf(scannedManifestFileNum));
        Tracers.record(EXTERNAL, prefix + tableIdentifier + "." + "skippedDataFilesNum", String.valueOf(skippedDataFilesNum));
        Tracers.record(EXTERNAL, prefix + tableIdentifier + "." + "resultedDataFilesNum", String.valueOf(resultedDataFilesNum));
        Tracers.record(EXTERNAL, prefix + tableIdentifier + "." + "manifestNumReadFromCache",
                String.valueOf(manifestNumReadFromCache));
        Tracers.record(EXTERNAL, prefix + tableIdentifier + "." + "manifestNumReadFromRemote",
                String.valueOf(manifestNumReadFromRemote));
        Tracers.record(EXTERNAL, prefix + "total.resultSplitsNum", String.valueOf(splits.size()));

        AtomicLong resultedTableFilesSize = new AtomicLong(0);
        for (Split split : splits) {
            List<DataFileMeta> dataFileMetas = ((DataSplit) split).dataFiles();
            dataFileMetas.forEach(dataFileMeta -> resultedTableFilesSize.addAndGet(dataFileMeta.fileSize()));
        }
        Tracers.record(EXTERNAL, prefix + tableIdentifier + "." + "resultedDataFilesSize", resultedTableFilesSize.get() + " B");
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit,
                                         TableVersionRange versionRange) {
        refreshDlfDataToken(((PaimonTable) table).getCatalogDBName(), ((PaimonTable) table).getCatalogTableName());
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

    public long getTableUpdateTime(String dbName, String tblName) {
        Identifier identifier = new Identifier(dbName, tblName);
        long lastCommitTime = -1;
        try {
            Optional<Snapshot> snapshotOptional = paimonNativeCatalog.getTable(identifier).latestSnapshot();
            if (snapshotOptional.isPresent()) {
                lastCommitTime = snapshotOptional.get().timeMillis();
            }
        } catch (Exception e) {
            LOG.error("Failed to get commit_time of paimon table {}.{}.", dbName, tblName, e);
        }
        if (lastCommitTime == -1) {
            lastCommitTime = System.currentTimeMillis();
        }
        return lastCommitTime;
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        PaimonTable paimonTable = (PaimonTable) table;
        Identifier identifier = new Identifier(paimonTable.getDbName(), paimonTable.getTableName());
        List<PartitionInfo> result = new ArrayList<>();
        if (table.isUnPartitioned()) {

            result.add(new Partition(paimonTable.getCatalogTableName(),
                    this.getTableUpdateTime(paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName()), null,
                    null, null));
            return result;
        }
        Map<String, Partition> partitionInfo = this.partitionInfos.get(identifier);
        for (String partitionName : partitionNames) {
            if (partitionInfo == null || partitionInfo.get(partitionName) == null) {
                this.updatePartitionInfo(paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName());
                partitionInfo = this.partitionInfos.get(identifier);
            }
            if (partitionInfo.get(partitionName) != null) {
                result.add(partitionInfo.get(partitionName));
            } else {
                LOG.warn("Cannot find the paimon partition info: {}", partitionName);
            }
        }
        return result;
    }

    public void refreshDlfDataToken(String dbName, String tblName) {
        String ramUser = DlfUtil.getRamUser();
        if (null == ramUser || ramUser.isEmpty() || paimonNativeCatalog.options().isEmpty()
                || !paimonNativeCatalog.options().get("metastore").equalsIgnoreCase("dlf-paimon")) {
            LOG.debug("Don't need to refresh for table {}.{} for user {}", dbName, tblName, ramUser);
            return;
        }
        DlfConfig config = new DlfConfig();

        Configuration conf = DlfUtil.readHadoopConf();
        config.put(DLF_ENDPOINT, paimonNativeCatalog.options().containsKey(DLF_ENDPOINT)
                ? paimonNativeCatalog.options().get(DLF_ENDPOINT) : conf.get(DLF_ENDPOINT));
        config.put(DLF_REGION, paimonNativeCatalog.options().containsKey(DLF_REGION)
                ? paimonNativeCatalog.options().get(DLF_REGION) : conf.get(DLF_REGION));
        config.put(META_CREDENTIAL_PROVIDER, paimonNativeCatalog.options().containsKey(META_CREDENTIAL_PROVIDER)
                ? paimonNativeCatalog.options().get(META_CREDENTIAL_PROVIDER) : conf.get(META_CREDENTIAL_PROVIDER));
        config.put(META_CREDENTIAL_PROVIDER_URL, paimonNativeCatalog.options().containsKey(META_CREDENTIAL_PROVIDER_URL)
                ? paimonNativeCatalog.options().get(META_CREDENTIAL_PROVIDER_URL) : conf.get(META_CREDENTIAL_PROVIDER_URL));

        DlfResource.Builder builder = DlfResource.builder()
                .catalogInstanceId(paimonNativeCatalog.options().get(CATALOG_ID))
                .databaseName(dbName);
        // For creating table operation, we don't need table name.
        if (!tblName.isEmpty()) {
            // For system table, remove the symbol
            tblName = tblName.replaceAll("\\$.*", "");
            builder.tableName(tblName);
        }
        DlfResource dlfResource = builder.build();

        try {
            String dataTokenName = getLocalDataTokenFile(ramUser, dbName, tblName);
            String dataTokenPath = paimonNativeCatalog.options().containsKey(DATA_CREDENTIAL_PROVIDER_URL)
                    ? paimonNativeCatalog.options().get(DATA_CREDENTIAL_PROVIDER_URL) : conf.get(DATA_CREDENTIAL_PROVIDER_URL);
            if (dataTokenPath.startsWith("secrets")) {
                // remove 'secrets://' prefix
                dataTokenPath = dataTokenPath.substring(10);
            }
            Path dataTokenDir = Paths.get(dataTokenPath);
            if (!Files.exists(dataTokenDir)) {
                LOG.debug("Creating data token parent dir {} for {}.{}", dataTokenPath, dbName, tblName);
                Files.createDirectories(dataTokenDir);
            }
            dataTokenName = dataTokenPath + dataTokenName;

            File dataTokenFile = new File(dataTokenName);
            boolean hasDataTokenFile = dataTokenFile.exists();
            DlfDataToken dataToken = new DlfDataToken();
            if (hasDataTokenFile) {
                ObjectMapper mapper = new ObjectMapper();
                dataToken = mapper.readValue(dataTokenFile, DlfDataToken.class);
            }
            // Check if the token has expired
            // todo: async refresh
            if (!hasDataTokenFile || dataToken.getExpiration().getTime() - System.currentTimeMillis()
                    < Config.dlf_data_token_refresh_check_interval_second * 1000) {
                DlfContext dlfContext = new DlfContext(config, "");
                DlfTokenClient dlfClient = dlfContext.getDlfTokenClient(ramUser);
                String dataTokenFilePath = dataTokenPath + dlfClient.getDataTokenIdentifier(dlfResource);
                if (!dataTokenName.equalsIgnoreCase(dataTokenFilePath)) {
                    LOG.warn(dataTokenName + " != " + dataTokenFilePath);
                    throw new StarRocksConnectorException("Accessing the wrong data token file " + dataTokenName);
                }
                DlfDataToken dlfDataToken = dlfClient.getDataToken(dlfResource);
                String dataTokenJson = dlfDataToken.toJson();
                BufferedWriter writer = new BufferedWriter(new FileWriter(dataTokenName));
                writer.write(dataTokenJson);
                writer.close();
                LOG.debug("Updated data token {} of {}.{} on {}", dataTokenJson, dbName, tblName, dataTokenName);
            }
        } catch (Exception e) {
            throw new StarRocksConnectorException(e.getMessage(), e);
        }
    }

    public String getLocalDataTokenFile(String ramUser, String dbName, String tblName) throws Exception {
        Identifier paimonIdentifier = new Identifier(dbName, tblName);
        String catalogID = DlfPaimonCatalog.from(this.paimonNativeCatalog).getCatalogUUid();
        String databaseID =  DlfPaimonCatalog.from(this.paimonNativeCatalog)
                .getDlfDatabase(dbName).getParameters().get("databaseUuid");

        String dataTokenName = ramUser + ":" + catalogID + ":" + databaseID;
        if (!tblName.isEmpty()) {
            org.apache.paimon.table.Table paimonTable = this.paimonNativeCatalog.getTable(paimonIdentifier);
            if (paimonTable instanceof FileStoreTable) {
                String tableID = ((FileStoreTable) paimonTable).schema().options().get("tableUuid");
                dataTokenName = dataTokenName + ":" + tableID;
            } else {
                // For ro table
                String tableID = paimonTable.options().get("tableUuid");
                if (null != tableID) {
                    dataTokenName = dataTokenName + ":" + tableID;
                }
            }
        }
        LOG.debug("Decoded data token file name is " + dataTokenName);
        dataTokenName = Base64Util.encodeBase64WithoutPadding(dataTokenName);
        return dataTokenName;
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
            } catch (Exception e) {
                LOG.error("Failed to refresh paimon partitions {}.{}.", catalogName, identifier.getFullName(), e);
                throw new StarRocksConnectorException(e.getMessage(), e);
            }
        } else {
            LOG.warn("Current catalog {} does not support cache.", catalogName);
        }
    }

    @Override
    public void finishSink(String dbName, String tblName, List<TSinkCommitInfo> commitInfos) {
        Identifier identifier = new Identifier(dbName, tblName);
        List<TPaimonCommitMessage> commitMessageList = commitInfos.stream()
                .map(TSinkCommitInfo::getPaimon_commit_message).collect(Collectors.toList());

        try {
            org.apache.paimon.table.Table paimonNativeTable = this.paimonNativeCatalog.getTable(identifier);
            BatchWriteBuilder builder = paimonNativeTable.newBatchWriteBuilder();

            if (commitInfos.get(0).isIs_overwrite()) {
                builder.withOverwrite(new HashMap<>());
            }
            BatchTableCommit commit = builder.newCommit();

            List<CommitMessage> messList = new ArrayList<>();
            CommitMessageSerializer commitMessageSerializer = new CommitMessageSerializer();

            for (TPaimonCommitMessage tPaimonCommitMessage : commitMessageList) {
                byte[] commitMessage = Base64.getDecoder().decode(tPaimonCommitMessage.getCommit_info_string_list());
                ByteArrayInputStream bis = new ByteArrayInputStream(commitMessage);
                List<CommitMessage> commitMessages = commitMessageSerializer.deserializeList(
                        commitMessageSerializer.getVersion(), new DataInputViewStreamWrapper(bis));
                messList.addAll(commitMessages);
            }
            commit.commit(messList);
            commit.close();
        } catch (Exception e) {
            throw new StarRocksConnectorException(e.getMessage(), e);
        }
    }

    public static boolean onlyHasPartitionPredicate(Table table, ScalarOperator predicate) {
        if (predicate == null) {
            return true;
        }

        List<ScalarOperator> scalarOperators = Utils.extractConjuncts(predicate);

        List<String> predicateColumns = new ArrayList<>();
        for (ScalarOperator operator : scalarOperators) {
            String columnName = null;
            if (operator.getChild(0) instanceof ColumnRefOperator) {
                columnName = ((ColumnRefOperator) operator.getChild(0)).getName();
            }

            if (columnName == null || columnName.isEmpty()) {
                return false;
            }

            predicateColumns.add(columnName);
        }

        List<String> partitionColNames = table.getPartitionColumnNames();
        for (String columnName : predicateColumns) {
            if (!partitionColNames.contains(columnName)) {
                return false;
            }
        }

        return true;
    }
}
