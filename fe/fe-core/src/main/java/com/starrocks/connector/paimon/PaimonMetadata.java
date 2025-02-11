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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.system.PartitionsTable;
import org.apache.paimon.table.system.SchemasTable;
import org.apache.paimon.table.system.SnapshotsTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class PaimonMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(PaimonMetadata.class);
    private final Catalog paimonNativeCatalog;
    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private final Map<Identifier, Table> tables = new ConcurrentHashMap<>();
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<PredicateSearchKey, PaimonSplitsInfo> paimonSplits = new ConcurrentHashMap<>();
    private final Map<String, Long> partitionInfos = new ConcurrentHashMap<>();
    private final ConnectorProperties properties;

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
    public List<String> listDbNames() {
        return paimonNativeCatalog.listDatabases();
    }

    @Override
    public List<String> listTableNames(String dbName) {
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

        Identifier partitionTableIdentifier = new Identifier(databaseName, String.format("%s%s", tableName, "$partitions"));
        RecordReaderIterator<InternalRow> iterator = null;
        try {
            PartitionsTable table = (PartitionsTable) paimonNativeCatalog.getTable(partitionTableIdentifier);
            RowType partitionTableRowType = table.rowType();
            DataType lastUpdateTimeType = partitionTableRowType.getTypeAt(partitionTableRowType
                    .getFieldIndex("last_update_time"));
            int[] projected = new int[] {0, 4};
            RecordReader<InternalRow> recordReader = table.newReadBuilder().withProjection(projected)
                    .newRead().createReader(table.newScan().plan());
            iterator = new RecordReaderIterator<>(recordReader);
            while (iterator.hasNext()) {
                InternalRow rowData = iterator.next();
                String partition = rowData.getString(0).toString();
                org.apache.paimon.data.Timestamp lastUpdateTime = rowData.getTimestamp(1,
                        DataTypeChecks.getPrecision(lastUpdateTimeType));
                String[] partitionValues = partition.replace("[", "").replace("]", "")
                        .split(",");
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
                this.partitionInfos.put(partitionName, lastUpdateTime.getMillisecond());
            }
        } catch (Exception e) {
            LOG.error("Failed to update partition info of paimon table {}.{}.", databaseName, tableName, e);
        } finally {
            if (iterator != null) {
                try {
                    iterator.close();
                } catch (Exception e) {
                    LOG.error("Failed to update partition info of paimon table {}.{}.", databaseName, tableName, e);
                }
            }
        }
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName, ConnectorMetadatRequestContext requestContext) {
        updatePartitionInfo(databaseName, tableName);
        return new ArrayList<>(this.partitionInfos.keySet());
    }

    @Override
    public Database getDb(String dbName) {
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
    public Table getTable(String dbName, String tblName) {
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
    public boolean tableExists(String dbName, String tableName) {
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
        if (paimonTable.getNativeTable().latestSnapshotId().isPresent()) {
            latestSnapshotId = paimonTable.getNativeTable().latestSnapshotId().getAsLong();
        }
        PredicateSearchKey filter = PredicateSearchKey.of(paimonTable.getCatalogDBName(),
                paimonTable.getCatalogTableName(), latestSnapshotId, params.getPredicate());
        if (!paimonSplits.containsKey(filter)) {
            ReadBuilder readBuilder = paimonTable.getNativeTable().newReadBuilder();
            int[] projected =
                    params.getFieldNames().stream().mapToInt(name -> (paimonTable.getFieldNames().indexOf(name))).toArray();
            List<Predicate> predicates = extractPredicates(paimonTable, params.getPredicate());
            List<Split> splits = readBuilder.withFilter(predicates).withProjection(projected).newScan().plan().splits();
            PaimonSplitsInfo paimonSplitsInfo = new PaimonSplitsInfo(predicates, splits);
            paimonSplits.put(filter, paimonSplitsInfo);
            List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(
                    PaimonRemoteFileDesc.createPamonRemoteFileDesc(paimonSplitsInfo));
            remoteFileInfo.setFiles(remoteFileDescs);
        } else {
            List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(
                    PaimonRemoteFileDesc.createPamonRemoteFileDesc(paimonSplits.get(filter)));
            remoteFileInfo.setFiles(remoteFileDescs);
        }

        return Lists.newArrayList(remoteFileInfo);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit,
                                         TableVersionRange versionRange) {
        if (!properties.enableGetTableStatsFromExternalMetadata()) {
            return StatisticsUtils.buildDefaultStatistics(columns.keySet());
        }

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
            Predicate equal = predicateBuilder.equal(predicateBuilder.indexOf("schema_id"), 0);
            RecordReader<InternalRow> recordReader = table.newReadBuilder().withProjection(projected)
                    .withFilter(equal).newRead().createReader(table.newScan().plan());
            iterator = new RecordReaderIterator<>(recordReader);
            while (iterator.hasNext()) {
                InternalRow rowData = iterator.next();
                Long schemaIdValue = rowData.getLong(0);
                org.apache.paimon.data.Timestamp updateTime = rowData
                        .getTimestamp(1, DataTypeChecks.getPrecision(updateTimeType));
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
                org.apache.paimon.data.Timestamp commitTime = rowData
                        .getTimestamp(0, DataTypeChecks.getPrecision(commitTimeType));
                if (commitTime.getMillisecond() > lastCommitTime) {
                    lastCommitTime = commitTime.getMillisecond();
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
                    this.getTableUpdateTime(paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName())));
            return result;
        }
        for (String partitionName : partitionNames) {
            if (this.partitionInfos.get(partitionName) == null) {
                this.updatePartitionInfo(paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName());
            }
            if (this.partitionInfos.get(partitionName) != null) {
                result.add(new Partition(partitionName, this.partitionInfos.get(partitionName)));
            } else {
                LOG.warn("Cannot find the paimon partition info: {}", partitionName);
            }
        }
        return result;
    }
}
