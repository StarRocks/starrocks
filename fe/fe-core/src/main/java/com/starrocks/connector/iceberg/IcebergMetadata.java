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


package com.starrocks.connector.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.cost.IcebergMetricsReporter;
import com.starrocks.connector.iceberg.cost.IcebergStatisticProvider;
import com.starrocks.sql.PlannerProfile;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.TIcebergDataFile;
import com.starrocks.thrift.TSinkCommitInfo;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.convertIcebergPartitionToPartitionName;
import static com.starrocks.connector.iceberg.IcebergApiConverter.parsePartitionFields;
import static com.starrocks.connector.iceberg.IcebergApiConverter.toIcebergApiSchema;
import static com.starrocks.connector.iceberg.IcebergCatalogType.GLUE_CATALOG;
import static com.starrocks.connector.iceberg.IcebergCatalogType.HIVE_CATALOG;
import static com.starrocks.connector.iceberg.IcebergCatalogType.REST_CATALOG;
import static com.starrocks.connector.iceberg.hive.IcebergHiveCatalog.LOCATION_PROPERTY;

public class IcebergMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(IcebergMetadata.class);
    private final String catalogName;
    private final IcebergCatalog icebergCatalog;
    private final IcebergStatisticProvider statisticProvider = new IcebergStatisticProvider();

    private final Map<TableIdentifier, Table> tables = new ConcurrentHashMap<>();
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<IcebergFilter, List<FileScanTask>> tasks = new ConcurrentHashMap<>();

    public IcebergMetadata(String catalogName, IcebergCatalog icebergCatalog) {
        this.catalogName = catalogName;
        this.icebergCatalog = icebergCatalog;
        new IcebergMetricsReporter().setThreadLocalReporter();
    }

    @Override
    public List<String> listDbNames() {
        return icebergCatalog.listAllDatabases();
    }

    @Override
    public void createDb(String dbName, Map<String, String> properties) throws AlreadyExistsException {
        if (dbExists(dbName)) {
            throw new AlreadyExistsException("Database Already Exists");
        }

        icebergCatalog.createDb(dbName, properties);
    }

    @Override
    public void dropDb(String dbName, boolean isForceDrop) throws MetaNotFoundException {
        if (listTableNames(dbName).size() != 0) {
            throw new StarRocksConnectorException("Database %s not empty", dbName);
        }

        icebergCatalog.dropDb(dbName);
        databases.remove(dbName);
    }

    @Override
    public Database getDb(String dbName) {
        if (databases.containsKey(dbName)) {
            return databases.get(dbName);
        }
        Database db;
        try {
            db = icebergCatalog.getDB(dbName);
        } catch (NoSuchNamespaceException e) {
            LOG.error("Database {} not found", dbName, e);
            return null;
        }

        databases.put(dbName, db);
        return db;
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return icebergCatalog.listTables(dbName);
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        Schema schema = toIcebergApiSchema(stmt.getColumns());
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        List<String> partitionColNames = partitionDesc == null ? Lists.newArrayList() :
                ((ListPartitionDesc) partitionDesc).getPartitionColNames();
        PartitionSpec partitionSpec = parsePartitionFields(schema, partitionColNames);
        Map<String, String> properties = stmt.getProperties() == null ? new HashMap<>() : stmt.getProperties();
        String tableLocation = properties.get(LOCATION_PROPERTY);
        Map<String, String> createTableProperties = IcebergApiConverter.rebuildCreateTableProperties(properties);

        return icebergCatalog.createTable(dbName, tableName, schema, partitionSpec, tableLocation, createTableProperties);
    }

    @Override
    public void dropTable(DropTableStmt stmt) {
        icebergCatalog.dropTable(stmt.getDbName(), stmt.getTableName(), stmt.isForceDrop());
        tables.remove(TableIdentifier.of(stmt.getDbName(), stmt.getTableName()));
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        TableIdentifier identifier = TableIdentifier.of(dbName, tblName);
        if (tables.containsKey(identifier)) {
            return tables.get(identifier);
        }

        try {
            IcebergCatalogType catalogType = icebergCatalog.getIcebergCatalogType();
            org.apache.iceberg.Table icebergTable = icebergCatalog.getTable(dbName, tblName);
            Table table = IcebergApiConverter.toIcebergTable(icebergTable, catalogName, dbName, tblName, catalogType.name());
            tables.put(identifier, table);
            return table;

        } catch (StarRocksConnectorException | NoSuchTableException e) {
            LOG.error("Failed to get iceberg table {}", identifier, e);
            return null;
        }
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName) {
        org.apache.iceberg.Table icebergTable = icebergCatalog.getTable(dbName, tblName);
        IcebergCatalogType nativeType = icebergCatalog.getIcebergCatalogType();

        if (nativeType != HIVE_CATALOG && nativeType != REST_CATALOG && nativeType != GLUE_CATALOG) {
            throw new StarRocksConnectorException(
                    "Do not support get partitions from catalog type: " + nativeType);
        }

        if (icebergTable.spec().fields().stream()
                .anyMatch(partitionField -> !partitionField.transform().isIdentity())) {
            throw new StarRocksConnectorException(
                    "Do not support get partitions from No-Identity partition transform now");
        }

        List<String> partitionNames = Lists.newArrayList();
        if (icebergTable.spec().isUnpartitioned()) {
            return partitionNames;
        }

        if (icebergTable.spec().fields().stream()
                .anyMatch(partitionField -> !partitionField.transform().isIdentity())) {
            return partitionNames;
        }
        TableScan tableScan = icebergTable.newScan();
        List<FileScanTask> tasks = Lists.newArrayList(tableScan.planFiles());

        for (FileScanTask fileScanTask : tasks) {
            StructLike partition = fileScanTask.file().partition();
            partitionNames.add(convertIcebergPartitionToPartitionName(icebergTable.spec(), partition));
        }
        return partitionNames;
    }

    @Override
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys,
                                                   long snapshotId, ScalarOperator predicate, List<String> fieldNames) {
        return getRemoteFileInfos((IcebergTable) table, snapshotId, predicate);
    }

    private List<RemoteFileInfo> getRemoteFileInfos(IcebergTable table, long snapshotId, ScalarOperator predicate) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        IcebergFilter key = IcebergFilter.of(table.getRemoteDbName(), table.getRemoteTableName(), snapshotId, predicate);

        if (!tasks.containsKey(key)) {
            List<ScalarOperator> scalarOperators = Utils.extractConjuncts(predicate);
            org.apache.iceberg.Table nativeTbl = table.getNativeTable();
            Types.StructType schema = nativeTbl.schema().asStruct();
            ScalarOperatorToIcebergExpr.IcebergContext icebergContext = new ScalarOperatorToIcebergExpr.IcebergContext(schema);
            Expression icebergPredicate = new ScalarOperatorToIcebergExpr().convert(scalarOperators, icebergContext);

            ImmutableList.Builder<FileScanTask> builder = ImmutableList.builder();
            org.apache.iceberg.Table nativeTable = table.getNativeTable();
            TableScan scan = nativeTable.newScan().useSnapshot(snapshotId);
            if (icebergPredicate.op() != Expression.Operation.TRUE) {
                scan = scan.filter(icebergPredicate);
            }

            CloseableIterable<CombinedScanTask> combinedScanTasks = scan.planTasks();
            for (CombinedScanTask combinedScanTask : combinedScanTasks) {
                for (FileScanTask fileScanTask : combinedScanTask.files()) {
                    builder.add(fileScanTask);
                }
            }
            try {
                combinedScanTasks.close();
            } catch (IOException e) {
                // Ignored
            }

            IcebergMetricsReporter.lastReport().ifPresent(scanReportWithCounter -> {
                PlannerProfile.addCustomProperties("Iceberg.Metadata.ScanMetrics." +
                                scanReportWithCounter.getScanReport().tableName() + " / No_" + scanReportWithCounter.getCount(),
                        scanReportWithCounter.getScanReport().scanMetrics().toString());
            });
            tasks.put(key, builder.build());
        }

        List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(RemoteFileDesc.createIcebergRemoteFileDesc(tasks.get(key)));
        remoteFileInfo.setFiles(remoteFileDescs);

        return Lists.newArrayList(remoteFileInfo);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate) {
        return statisticProvider.getTableStatistics((IcebergTable) table, predicate, columns);
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        org.apache.iceberg.Table nativeTable = ((IcebergTable) table).getNativeTable();
        try {
            if (nativeTable instanceof BaseTable) {
                BaseTable baseTable = (BaseTable) nativeTable;
                if (baseTable.operations().refresh() == null) {
                    // If table is loaded successfully, current table metadata will never be null.
                    // So when we get a null metadata after refresh, it indicates the table has been dropped.
                    // See: https://github.com/StarRocks/starrocks/issues/3076
                    throw new NoSuchTableException("No such table: %s", nativeTable.name());
                }
            } else {
                // table loaded by GlobalStateMgr should be a base table
                throw new StarRocksConnectorException("Invalid table type of %s, it should be a BaseTable!", nativeTable.name());
            }
        } catch (NoSuchTableException e) {
            throw new StarRocksConnectorException("No such table  %s", nativeTable.name());
        } catch (IllegalStateException ei) {
            throw new StarRocksConnectorException("Refresh table %s with failure, the table under hood" +
                    " may have been dropped. You should re-create the external table. cause %s",
                    nativeTable.name(), ei.getMessage());
        }
    }

    @Override
    public void finishSink(String dbName, String tableName, List<TSinkCommitInfo> commitInfos) {
        boolean isOverwrite = false;
        if (!commitInfos.isEmpty()) {
            TSinkCommitInfo sinkCommitInfo = commitInfos.get(0);
            if (sinkCommitInfo.isSetIs_overwrite()) {
                isOverwrite = sinkCommitInfo.is_overwrite;
            }
        }

        List<TIcebergDataFile> dataFiles = commitInfos.stream()
                .map(TSinkCommitInfo::getIceberg_data_file).collect(Collectors.toList());

        IcebergTable table = (IcebergTable) getTable(dbName, tableName);
        org.apache.iceberg.Table nativeTbl = table.getNativeTable();
        Transaction transaction = nativeTbl.newTransaction();
        BatchWrite batchWrite = getBatchWrite(transaction, isOverwrite);

        PartitionSpec partitionSpec = nativeTbl.spec();
        for (TIcebergDataFile dataFile : dataFiles) {
            Metrics metrics = IcebergApiConverter.buildDataFileMetrics(dataFile);
            DataFiles.Builder builder =
                    DataFiles.builder(partitionSpec)
                            .withMetrics(metrics)
                            .withPath(dataFile.path)
                            .withFormat(dataFile.format)
                            .withRecordCount(dataFile.record_count)
                            .withFileSizeInBytes(dataFile.file_size_in_bytes)
                            .withSplitOffsets(dataFile.split_offsets);

            if (partitionSpec.isPartitioned()) {
                String relativePartitionLocation = getIcebergRelativePartitionPath(
                        nativeTbl.location(), dataFile.partition_path);

                PartitionData partitionData = partitionDataFromPath(
                        relativePartitionLocation, partitionSpec);
                builder.withPartition(partitionData);
            }
            batchWrite.addFile(builder.build());
        }

        try {
            batchWrite.commit();
            transaction.commitTransaction();
        } catch (Exception e) {
            List<String> toDeleteFiles = dataFiles.stream()
                    .map(TIcebergDataFile::getPath)
                    .collect(Collectors.toList());
            icebergCatalog.deleteUncommittedDataFiles(toDeleteFiles);
            LOG.error("Failed to commit iceberg transaction on {}.{}", dbName, tableName, e);
            throw new StarRocksConnectorException(e.getMessage());
        }
    }

    public BatchWrite getBatchWrite(Transaction transaction, boolean isOverwrite) {
        return isOverwrite ? new DynamicOverwrite(transaction) : new Append(transaction);
    }

    public static PartitionData partitionDataFromPath(String relativePartitionPath, PartitionSpec spec) {
        PartitionData data = new PartitionData(spec.fields().size());
        String[] partitions = relativePartitionPath.split("/", -1);
        List<PartitionField> partitionFields = spec.fields();

        for (int i = 0; i < partitions.length; i++) {
            PartitionField field = partitionFields.get(i);
            String[] parts = partitions[i].split("=", 2);
            Preconditions.checkArgument(parts.length == 2 && parts[0] != null &&
                    field.name().equals(parts[0]), "Invalid partition: %s", partitions[i]);

            org.apache.iceberg.types.Type sourceType = spec.partitionType().fields().get(i).type();
            data.set(i, Conversions.fromPartitionString(sourceType, parts[1]));
        }
        return data;
    }

    public static String getIcebergRelativePartitionPath(String tableLocation, String partitionLocation) {
        tableLocation = tableLocation.endsWith("/") ? tableLocation.substring(0, tableLocation.length() - 1) : tableLocation;
        String tableLocationWithData = tableLocation + "/data/";
        String path = PartitionUtil.getSuffixName(tableLocationWithData, partitionLocation);
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        return path;
    }

    @Override
    public void clear() {
        tasks.clear();
        tables.clear();
        IcebergMetricsReporter.remove();
    }

    interface BatchWrite {
        void addFile(DataFile file);
        void commit();
    }

    static class Append implements BatchWrite {
        private final AppendFiles append;

        public Append(Transaction txn) {
            append = txn.newAppend();
        }

        @Override
        public void addFile(DataFile file) {
            append.appendFile(file);
        }

        @Override
        public void commit() {
            append.commit();
        }
    }

    static class DynamicOverwrite implements BatchWrite {
        private final ReplacePartitions replace;

        public DynamicOverwrite(Transaction txn) {
            replace = txn.newReplacePartitions();
        }

        @Override
        public void addFile(DataFile file) {
            replace.addFile(file);
        }

        @Override
        public void commit() {
            replace.commit();
        }
    }

    public static class PartitionData implements StructLike {
        private final Object[] values;

        private PartitionData(int size) {
            this.values = new Object[size];
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public <T> T get(int pos, Class<T> javaClass) {
            return javaClass.cast(values[pos]);
        }

        @Override
        public <T> void set(int pos, T value) {
            if (value instanceof ByteBuffer) {
                ByteBuffer buffer = (ByteBuffer) value;
                byte[] bytes = new byte[buffer.remaining()];
                buffer.duplicate().get(bytes);
                values[pos] = bytes;
            } else {
                values[pos] = value;
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            PartitionData that = (PartitionData) other;
            return Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}
