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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorTableVersion;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetaPreparationItem;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.PlanMode;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.RemoteMetaSplit;
import com.starrocks.connector.SerializedMetaSpec;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.cost.IcebergMetricsReporter;
import com.starrocks.connector.iceberg.cost.IcebergStatisticProvider;
import com.starrocks.connector.iceberg.io.IcebergCachingFileIO;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.connector.share.iceberg.SerializableTable;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TIcebergDataFile;
import com.starrocks.thrift.TSinkCommitInfo;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StarRocksIcebergTableScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.iceberg.view.View;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starrocks.catalog.Table.TableType.ICEBERG;
import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static com.starrocks.connector.ColumnTypeConverter.fromIcebergType;
import static com.starrocks.connector.PartitionUtil.createPartitionKeyWithType;
import static com.starrocks.connector.iceberg.IcebergApiConverter.filterManifests;
import static com.starrocks.connector.iceberg.IcebergApiConverter.mayHaveEqualityDeletes;
import static com.starrocks.connector.iceberg.IcebergApiConverter.parsePartitionFields;
import static com.starrocks.connector.iceberg.IcebergApiConverter.toIcebergApiSchema;
import static com.starrocks.connector.iceberg.IcebergCatalogType.GLUE_CATALOG;
import static com.starrocks.connector.iceberg.IcebergCatalogType.HIVE_CATALOG;
import static com.starrocks.connector.iceberg.IcebergCatalogType.REST_CATALOG;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;
import static java.util.Comparator.comparing;
import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE_DEFAULT;

public class IcebergMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(IcebergMetadata.class);

    public static final String LOCATION_PROPERTY = "location";
    public static final String FILE_FORMAT = "file_format";
    public static final String COMPRESSION_CODEC = "compression_codec";
    public static final String COMMENT = "comment";

    private final String catalogName;
    private final HdfsEnvironment hdfsEnvironment;
    private final IcebergCatalog icebergCatalog;
    private final IcebergStatisticProvider statisticProvider = new IcebergStatisticProvider();

    private final Map<TableIdentifier, org.apache.iceberg.Table> tables = new ConcurrentHashMap<>();
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<PredicateSearchKey, List<FileScanTask>> splitTasks = new ConcurrentHashMap<>();
    private final Set<PredicateSearchKey> scannedTables = new HashSet<>();
    private final Set<PredicateSearchKey> preparedTables = ConcurrentHashMap.newKeySet();

    private final Map<IcebergRemoteFileInfoSourceKey, RemoteFileInfoSource> remoteFileInfoSources = new ConcurrentHashMap<>();

    // FileScanTaskSchema -> Pair<schema_string, partition_string>
    private final Map<FileScanTaskSchema, Pair<String, String>> fileScanTaskSchemas = new ConcurrentHashMap<>();
    private final ExecutorService jobPlanningExecutor;
    private final ExecutorService refreshOtherFeExecutor;
    private final IcebergMetricsReporter metricsReporter;
    private final IcebergCatalogProperties catalogProperties;
    private final ConnectorProperties properties;

    public IcebergMetadata(String catalogName, HdfsEnvironment hdfsEnvironment, IcebergCatalog icebergCatalog,
                           ExecutorService jobPlanningExecutor, ExecutorService refreshOtherFeExecutor,
                           IcebergCatalogProperties catalogProperties) {
        this(catalogName, hdfsEnvironment, icebergCatalog, jobPlanningExecutor, refreshOtherFeExecutor,
                catalogProperties, new ConnectorProperties(ConnectorType.ICEBERG));
    }

    public IcebergMetadata(String catalogName, HdfsEnvironment hdfsEnvironment, IcebergCatalog icebergCatalog,
                           ExecutorService jobPlanningExecutor, ExecutorService refreshOtherFeExecutor,
                           IcebergCatalogProperties catalogProperties, ConnectorProperties properties) {
        this.catalogName = catalogName;
        this.hdfsEnvironment = hdfsEnvironment;
        this.icebergCatalog = icebergCatalog;
        this.metricsReporter = new IcebergMetricsReporter();
        this.jobPlanningExecutor = jobPlanningExecutor;
        this.refreshOtherFeExecutor = refreshOtherFeExecutor;
        this.catalogProperties = catalogProperties;
        this.properties = properties;
    }

    @Override
    public Table.TableType getTableType() {
        return ICEBERG;
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

        icebergCatalog.createDB(dbName, properties);
    }

    @Override
    public void dropDb(String dbName, boolean isForceDrop) throws MetaNotFoundException {
        if (listTableNames(dbName).size() != 0) {
            throw new StarRocksConnectorException("Database %s not empty", dbName);
        }

        icebergCatalog.dropDB(dbName);
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
        String comment = stmt.getComment();
        if (comment != null && !comment.isEmpty()) {
            properties.put(COMMENT, comment);
        }
        Map<String, String> createTableProperties = IcebergApiConverter.rebuildCreateTableProperties(properties);

        return icebergCatalog.createTable(dbName, tableName, schema, partitionSpec, tableLocation, createTableProperties);
    }

    @Override
    public void createView(CreateViewStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String viewName = stmt.getTable();

        Database db = getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        if (getView(dbName, viewName) != null) {
            if (stmt.isSetIfNotExists()) {
                LOG.info("create view[{}] which already exists", viewName);
                return;
            } else if (stmt.isReplace()) {
                LOG.info("view {} already exists, need to replace it", viewName);
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, viewName);
            }
        }

        ConnectorViewDefinition viewDefinition = ConnectorViewDefinition.fromCreateViewStmt(stmt);
        icebergCatalog.createView(catalogName, viewDefinition, stmt.isReplace());
    }

    @Override
    public void alterView(AlterViewStmt stmt) throws StarRocksException {
        String dbName = stmt.getDbName();
        String viewName = stmt.getTable();

        Database db = getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        if (getView(dbName, viewName) == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, dbName + "." + viewName);
        }

        ConnectorViewDefinition viewDefinition = ConnectorViewDefinition.fromAlterViewStmt(stmt);
        View currentView = icebergCatalog.getView(dbName, viewName);
        if (!stmt.getProperties().isEmpty() || stmt.isAlterDialect()) {
            icebergCatalog.alterView(currentView, viewDefinition);
        } else {
            throw new DdlException("ALTER VIEW <viewName> AS is not supported. Use CREATE OR REPLACE VIEW instead");
        }
    }

    @Override
    public void alterTable(ConnectContext context, AlterTableStmt stmt) throws StarRocksException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        org.apache.iceberg.Table table = icebergCatalog.getTable(dbName, tableName);

        if (table == null) {
            throw new StarRocksConnectorException(
                    "Failed to load iceberg table: " + stmt.getTbl().toString());
        }

        IcebergAlterTableExecutor executor = new IcebergAlterTableExecutor(stmt, table, icebergCatalog, hdfsEnvironment);
        executor.execute();

        synchronized (this) {
            tables.remove(TableIdentifier.of(dbName, tableName));
            try {
                icebergCatalog.refreshTable(dbName, tableName, jobPlanningExecutor);
            } catch (Exception exception) {
                LOG.error("Failed to refresh caching iceberg table.");
                icebergCatalog.invalidateCache(dbName, tableName);
            }
            asyncRefreshOthersFeMetadataCache(dbName, tableName);
        }
    }

    @Override
    public void dropTable(DropTableStmt stmt) {
        Table icebergTable = getTable(stmt.getDbName(), stmt.getTableName());

        if (icebergTable != null && icebergTable.isIcebergView()) {
            icebergCatalog.dropView(stmt.getDbName(), stmt.getTableName());
            return;
        }

        if (icebergTable == null) {
            return;
        }

        icebergCatalog.dropTable(stmt.getDbName(), stmt.getTableName(), stmt.isForceDrop());
        tables.remove(TableIdentifier.of(stmt.getDbName(), stmt.getTableName()));
        StatisticUtils.dropStatisticsAfterDropTable(icebergTable);
        asyncRefreshOthersFeMetadataCache(stmt.getDbName(), stmt.getTableName());
    }

    public void updateTableProperty(Database db, IcebergTable icebergTable) {
        Map<String, String> properties = new HashMap(icebergTable.getNativeTable().properties());
        List<UniqueConstraint> uniqueConstraints = PropertyAnalyzer.analyzeUniqueConstraint(properties, db, icebergTable);
        icebergTable.setUniqueConstraints(uniqueConstraints);
        List<ForeignKeyConstraint> foreignKeyConstraints =
                PropertyAnalyzer.analyzeForeignKeyConstraint(properties, db, icebergTable);
        icebergTable.setForeignKeyConstraints(foreignKeyConstraints);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        TableIdentifier identifier = TableIdentifier.of(dbName, tblName);

        try {
            org.apache.iceberg.Table icebergTable;
            if (tables.containsKey(identifier)) {
                icebergTable = tables.get(identifier);
            } else {
                icebergTable = icebergCatalog.getTable(dbName, tblName);
            }

            IcebergCatalogType catalogType = icebergCatalog.getIcebergCatalogType();
            // Hive/Glue catalog table name is case-insensitive, normalize it to lower case
            if (catalogType == IcebergCatalogType.HIVE_CATALOG || catalogType == IcebergCatalogType.GLUE_CATALOG) {
                dbName = dbName.toLowerCase();
                tblName = tblName.toLowerCase();
            }
            Database db = getDb(dbName);
            IcebergTable table =
                    IcebergApiConverter.toIcebergTable(icebergTable, catalogName, dbName, tblName, catalogType.name());
            table.setComment(icebergTable.properties().getOrDefault(COMMENT, ""));
            tables.put(identifier, icebergTable);
            updateTableProperty(db, table);
            return table;
        } catch (StarRocksConnectorException e) {
            LOG.error("Failed to get iceberg table {}", identifier, e);
            return null;
        } catch (NoSuchTableException e) {
            return getView(dbName, tblName);
        }
    }

    public static long getSnapshotIdFromVersion(org.apache.iceberg.Table table, ConnectorTableVersion version) {
        switch (version.getPointerType()) {
            case TEMPORAL:
                return getSnapshotIdFromTemporalVersion(table, version.getConstantOperator());
            case VERSION:
                return getTargetSnapshotIdFromVersion(table, version.getConstantOperator());
            case UNKNOWN:
            default:
                throw new StarRocksConnectorException("Unknown version type %s", version.getPointerType());
        }
    }

    private static long getTargetSnapshotIdFromVersion(org.apache.iceberg.Table table, ConstantOperator version) {
        long snapshotId;
        if (version.getType() == com.starrocks.catalog.Type.BIGINT) {
            snapshotId = version.getBigint();
        } else if (version.getType() == com.starrocks.catalog.Type.VARCHAR) {
            String refName = version.getVarchar();
            SnapshotRef ref = table.refs().get(refName);
            if (ref == null) {
                throw new StarRocksConnectorException("Cannot find snapshot with reference name: " + refName);
            }
            snapshotId = ref.snapshotId();
        } else {
            throw new StarRocksConnectorException("Unsupported type for table version: " + version);
        }

        if (table.snapshot(snapshotId) == null) {
            throw new StarRocksConnectorException("Iceberg snapshot ID does not exists: " + snapshotId);
        }
        return snapshotId;
    }

    private static long getSnapshotIdFromTemporalVersion(org.apache.iceberg.Table table, ConstantOperator version) {
        try {
            if (version.getType() != com.starrocks.catalog.Type.DATETIME &&
                    version.getType() != com.starrocks.catalog.Type.DATE &&
                    version.getType() != com.starrocks.catalog.Type.VARCHAR) {
                throw new StarRocksConnectorException("Unsupported type for table temporal version: %s." +
                        " You should use timestamp type", version);
            }
            Optional<ConstantOperator> timestampVersion = version.castTo(com.starrocks.catalog.Type.DATETIME);
            if (timestampVersion.isEmpty()) {
                throw new StarRocksConnectorException("Unsupported type for table temporal version: %s." +
                        " You should use timestamp type", version);
            }
            LocalDateTime time = timestampVersion.get().getDatetime();
            long epochMillis = Duration.ofSeconds(time.atZone(TimeUtils.getTimeZone().toZoneId()).toEpochSecond()).toMillis();
            return getSnapshotIdAsOfTime(table, epochMillis);
        } catch (Exception e) {
            LOG.error("Invalid temporal version {}", version, e);
            throw new StarRocksConnectorException("Invalid temporal version [%s]", version, e);
        }
    }

    public static long getSnapshotIdAsOfTime(org.apache.iceberg.Table table, long epochMillis) {
        return table.history().stream()
                .filter(logEntry -> logEntry.timestampMillis() <= epochMillis)
                .max(comparing(HistoryEntry::timestampMillis))
                .orElseThrow(() -> new StarRocksConnectorException("No version history table %s at or before %s",
                        table.name(), Instant.ofEpochMilli(epochMillis)))
                .snapshotId();
    }

    public Table getView(String dbName, String viewName) {
        try {
            View icebergView = icebergCatalog.getView(dbName, viewName);
            return IcebergApiConverter.toView(catalogName, dbName, icebergView);
        } catch (Exception e) {
            LOG.error("Failed to get iceberg view {}.{}", dbName, viewName, e);
            return null;
        }
    }

    @Override
    public TableVersionRange getTableVersionRange(String dbName, Table table,
                                                  Optional<ConnectorTableVersion> startVersion,
                                                  Optional<ConnectorTableVersion> endVersion) {
        if (startVersion.isPresent()) {
            throw new StarRocksConnectorException("Read table with start version is not supported");
        }

        if (endVersion.isEmpty()) {
            IcebergTable icebergTable = (IcebergTable) table;
            Optional<Long> snapshotId = Optional.ofNullable(icebergTable.getNativeTable().currentSnapshot())
                    .map(Snapshot::snapshotId);
            return TableVersionRange.withEnd(snapshotId);
        } else {
            Long snapshotId = getSnapshotIdFromVersion(((IcebergTable) table).getNativeTable(), endVersion.get());
            return TableVersionRange.withEnd(Optional.of(snapshotId));
        }
    }

    @Override
    public boolean tableExists(String dbName, String tblName) {
        return icebergCatalog.tableExists(dbName, tblName);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, ConnectorMetadatRequestContext requestContext) {
        IcebergCatalogType nativeType = icebergCatalog.getIcebergCatalogType();

        if (nativeType != HIVE_CATALOG && nativeType != REST_CATALOG && nativeType != GLUE_CATALOG) {
            throw new StarRocksConnectorException(
                    "Do not support get partitions from catalog type: " + nativeType);
        }

        Table table = getTable(dbName, tblName);
        return icebergCatalog.listPartitionNames((IcebergTable) table, requestContext, jobPlanningExecutor);
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        TableVersionRange version = params.getTableVersionRange();
        long snapshotId = version.end().isPresent() ? version.end().get() : -1;
        return getRemoteFiles((IcebergTable) table, snapshotId, params.getPredicate(), params.getLimit());
    }

    private List<RemoteFileInfo> getRemoteFiles(IcebergTable table, long snapshotId,
                                                ScalarOperator predicate, long limit) {
        String dbName = table.getCatalogDBName();
        String tableName = table.getCatalogTableName();

        PredicateSearchKey key = PredicateSearchKey.of(dbName, tableName, snapshotId, predicate);
        triggerIcebergPlanFilesIfNeeded(key, table, predicate, limit);

        List<FileScanTask> icebergScanTasks = splitTasks.get(key);
        if (icebergScanTasks == null) {
            throw new StarRocksConnectorException("Missing iceberg split task for table:[{}.{}]. predicate:[{}]",
                    dbName, tableName, predicate);
        }

        return icebergScanTasks.stream().map(IcebergRemoteFileInfo::new).collect(Collectors.toList());
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        List<Partition> ans =
                icebergCatalog.getPartitionsByNames((IcebergTable) table, null, partitionNames);
        return new ArrayList<>(ans);
    }

    @Override
    public boolean prepareMetadata(MetaPreparationItem item, Tracers tracers, ConnectContext connectContext) {
        PredicateSearchKey key;
        IcebergTable icebergTable;
        icebergTable = (IcebergTable) item.getTable();
        String dbName = icebergTable.getCatalogDBName();
        String tableName = icebergTable.getCatalogTableName();
        TableVersionRange versionRange = item.getVersion();
        if (versionRange.end().isEmpty()) {
            return true;
        }

        key = PredicateSearchKey.of(dbName, tableName, versionRange.end().get(), item.getPredicate());
        if (!preparedTables.add(key)) {
            return true;
        }

        // metadata collect has been triggered by other rules or process if contains
        if (!scannedTables.contains(key)) {
            synchronized (this) {
                try (Timer ignored = Tracers.watchScope(tracers, EXTERNAL, "ICEBERG.prepareTablesNum")) {
                    // Only record the number of tables that need to be prepared in parallel
                }
            }
        }
        if (connectContext == null) {
            connectContext = ConnectContext.get();
        }

        triggerIcebergPlanFilesIfNeeded(key, icebergTable, item.getPredicate(), item.getLimit(), tracers, connectContext);
        return true;
    }

    @Override
    public SerializedMetaSpec getSerializedMetaSpec(String dbName, String tableName, long snapshotId, String serializedPredicate,
                                                    MetadataTableType metadataTableType) {
        List<RemoteMetaSplit> remoteMetaSplits = new ArrayList<>();
        IcebergTable icebergTable = (IcebergTable) getTable(dbName, tableName);
        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
        if (snapshotId == -1) {
            Snapshot currentSnapshot = nativeTable.currentSnapshot();
            if (currentSnapshot == null) {
                return IcebergMetaSpec.EMPTY;
            } else {
                snapshotId = nativeTable.currentSnapshot().snapshotId();
            }
        }
        Snapshot snapshot = nativeTable.snapshot(snapshotId);

        Expression predicate = Expressions.alwaysTrue();
        if (!Strings.isNullOrEmpty(serializedPredicate)) {
            predicate = SerializationUtil.deserializeFromBase64(serializedPredicate);
        }

        FileIO fileIO = nativeTable.io();
        if (fileIO instanceof IcebergCachingFileIO) {
            fileIO = ((IcebergCachingFileIO) fileIO).getWrappedIO();
        }

        String serializedTable = SerializationUtil.serializeToBase64(new SerializableTable(nativeTable, fileIO));

        if (IcebergMetaSplit.onlyNeedSingleSplit(metadataTableType)) {
            return new IcebergMetaSpec(serializedTable, List.of(IcebergMetaSplit.placeholderSplit()), false);
        }

        List<ManifestFile> dataManifests = snapshot.dataManifests(nativeTable.io());

        List<ManifestFile> matchingDataManifests = filterManifests(dataManifests, nativeTable, predicate);
        for (ManifestFile file : matchingDataManifests) {
            remoteMetaSplits.add(IcebergMetaSplit.from(file));
        }

        List<ManifestFile> deleteManifests = snapshot.deleteManifests(nativeTable.io());
        List<ManifestFile> matchingDeleteManifests = filterManifests(deleteManifests, nativeTable, predicate);
        if (metadataTableType == MetadataTableType.FILES || metadataTableType == MetadataTableType.PARTITIONS) {
            for (ManifestFile file : matchingDeleteManifests) {
                remoteMetaSplits.add(IcebergMetaSplit.from(file));
            }
            return new IcebergMetaSpec(serializedTable, remoteMetaSplits, false);
        }

        boolean loadColumnStats = enableCollectColumnStatistics(ConnectContext.get()) ||
                (!matchingDeleteManifests.isEmpty() && mayHaveEqualityDeletes(snapshot) &&
                        catalogProperties.enableDistributedPlanLoadColumnStatsWithEqDelete());

        return new IcebergMetaSpec(serializedTable, remoteMetaSplits, loadColumnStats);
    }

    private void triggerIcebergPlanFilesIfNeeded(PredicateSearchKey key, IcebergTable table,
                                                 ScalarOperator predicate, long limit) {
        triggerIcebergPlanFilesIfNeeded(key, table, predicate, limit, null, ConnectContext.get());
    }

    private void triggerIcebergPlanFilesIfNeeded(PredicateSearchKey key, IcebergTable table, ScalarOperator predicate,
                                                 long limit, Tracers tracers, ConnectContext connectContext) {
        if (!scannedTables.contains(key)) {
            tracers = tracers == null ? Tracers.get() : tracers;
            try (Timer ignored = Tracers.watchScope(tracers, EXTERNAL, "ICEBERG.processSplit." + key)) {
                collectTableStatisticsAndCacheIcebergSplit(key, table, predicate, limit, tracers, connectContext);
            }
        }
    }

    public List<PartitionKey> getPrunedPartitions(Table table, ScalarOperator predicate, long limit, TableVersionRange version) {
        IcebergTable icebergTable = (IcebergTable) table;
        String dbName = icebergTable.getCatalogDBName();
        String tableName = icebergTable.getCatalogTableName();
        if (version.end().isEmpty()) {
            return new ArrayList<>();
        }

        PredicateSearchKey key = PredicateSearchKey.of(dbName, tableName, version.end().get(), predicate);
        triggerIcebergPlanFilesIfNeeded(key, icebergTable, predicate, limit);

        List<PartitionKey> partitionKeys = new ArrayList<>();
        List<FileScanTask> icebergSplitTasks = splitTasks.get(key);
        if (icebergSplitTasks == null) {
            throw new StarRocksConnectorException("Missing iceberg split task for table:[{}.{}]. predicate:[{}]",
                    dbName, tableName, predicate);
        }

        Set<List<String>> scannedPartitions = new HashSet<>();
        PartitionSpec spec = icebergTable.getNativeTable().spec();
        List<Column> partitionColumns = icebergTable.getPartitionColumnsIncludeTransformed();
        boolean existPartitionTransformedEvolution = ((IcebergTable) table).hasPartitionTransformedEvolution();
        for (FileScanTask fileScanTask : icebergSplitTasks) {
            org.apache.iceberg.PartitionData partitionData = (org.apache.iceberg.PartitionData) fileScanTask.file().partition();
            List<String> values = PartitionUtil.getIcebergPartitionValues(
                    spec, partitionData, existPartitionTransformedEvolution);

            if (values.size() != partitionColumns.size()) {
                // ban partition evolution and non-identify column.
                continue;
            }

            if (scannedPartitions.contains(values)) {
                continue;
            } else {
                scannedPartitions.add(values);
            }

            try {
                List<com.starrocks.catalog.Type> srTypes = new ArrayList<>();
                for (PartitionField partitionField : spec.fields()) {
                    if (partitionField.transform().isVoid()) {
                        continue;
                    }

                    if (!partitionField.transform().isIdentity()) {
                        Type sourceType = spec.schema().findType(partitionField.sourceId());
                        Type resultType = partitionField.transform().getResultType(sourceType);
                        if (resultType == Types.DateType.get()) {
                            resultType = Types.IntegerType.get();
                        }
                        srTypes.add(fromIcebergType(resultType));
                        continue;
                    }

                    srTypes.add(icebergTable.getColumn(icebergTable.getPartitionSourceName(spec.schema(),
                            partitionField)).getType());
                }

                if (existPartitionTransformedEvolution) {
                    srTypes = partitionColumns.stream()
                            .map(Column::getType)
                            .collect(Collectors.toList());
                }

                partitionKeys.add(createPartitionKeyWithType(values, srTypes, table.getType()));
            } catch (Exception e) {
                LOG.error("create partition key failed.", e);
                throw new StarRocksConnectorException(e.getMessage());
            }
        }

        return partitionKeys;
    }

    private void collectTableStatisticsAndCacheIcebergSplit(PredicateSearchKey key, Table table,
                                                            ScalarOperator predicate, long limit, Tracers tracers,
                                                            ConnectContext connectContext) {
        IcebergTable icebergTable = (IcebergTable) table;
        long snapshotId = key.getSnapshotId();
        // empty table
        if (snapshotId == -1) {
            return;
        }

        String dbName = icebergTable.getCatalogDBName();
        String tableName = icebergTable.getCatalogTableName();

        org.apache.iceberg.Table nativeTbl = icebergTable.getNativeTable();
        Types.StructType schema = nativeTbl.schema().asStruct();

        List<ScalarOperator> scalarOperators = Utils.extractConjuncts(predicate);
        ScalarOperatorToIcebergExpr.IcebergContext icebergContext = new ScalarOperatorToIcebergExpr.IcebergContext(schema);
        Expression icebergPredicate = new ScalarOperatorToIcebergExpr().convert(scalarOperators, icebergContext);

        boolean enableCollectColumnStatistics = enableCollectColumnStatistics(connectContext);

        Iterator<FileScanTask> iterator =
                buildFileScanTaskIterator((IcebergTable) table, icebergPredicate, snapshotId,
                        connectContext, enableCollectColumnStatistics);

        List<FileScanTask> icebergScanTasks = Lists.newArrayList();

        while (iterator.hasNext()) {
            FileScanTask scanTask = iterator.next();

            FileScanTask icebergSplitScanTask = scanTask;
            if (enableCollectColumnStatistics(connectContext)) {
                try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.buildSplitScanTask")) {
                    icebergSplitScanTask = buildIcebergSplitScanTask(scanTask, icebergPredicate, key);
                }

                List<Types.NestedField> fullColumns = nativeTbl.schema().columns();
                Map<Integer, Type.PrimitiveType> idToTypeMapping = fullColumns.stream()
                        .filter(column -> column.type().isPrimitiveType())
                        .collect(Collectors.toMap(Types.NestedField::fieldId, column -> column.type().asPrimitiveType()));

                Set<Integer> identityPartitionIds = nativeTbl.spec().fields().stream()
                        .filter(x -> x.transform().isIdentity())
                        .map(PartitionField::sourceId)
                        .collect(Collectors.toSet());

                List<Types.NestedField> nonPartitionPrimitiveColumns = fullColumns.stream()
                        .filter(column -> !identityPartitionIds.contains(column.fieldId()) &&
                                column.type().isPrimitiveType())
                        .collect(toImmutableList());

                try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.updateIcebergFileStats")) {
                    statisticProvider.updateIcebergFileStats(
                            icebergTable, scanTask, idToTypeMapping, nonPartitionPrimitiveColumns, key);
                }
            }

            icebergScanTasks.add(icebergSplitScanTask);
        }

        Optional<ScanReport> metrics = metricsReporter.getReporter(
                catalogName, dbName, tableName, snapshotId, icebergPredicate, nativeTbl);

        Tracers.Module module = Tracers.Module.EXTERNAL;
        if (metrics.isPresent()) {
            String name = "ICEBERG.ScanMetrics." + metrics.get().tableName() + "[" + icebergPredicate + "]";
            String value = metrics.get().scanMetrics().toString();
            if (tracers == null) {
                Tracers.record(module, name, value);
            } else {
                synchronized (this) {
                    Tracers.record(tracers, module, name, value);
                }
            }
        }

        splitTasks.put(key, icebergScanTasks);
        scannedTables.add(key);
    }

    @Override
    public RemoteFileInfoSource getRemoteFilesAsync(Table table, GetRemoteFilesParams params) {
        IcebergGetRemoteFilesParams param = params.cast();
        Optional<Long> snapshotId = param.getTableVersionRange().end();
        if (snapshotId.isEmpty()) {
            return RemoteFileInfoDefaultSource.EMPTY;
        }

        IcebergTable icebergTable = (IcebergTable) table;
        List<ScalarOperator> scalarOperators = Utils.extractConjuncts(params.getPredicate());
        ScalarOperatorToIcebergExpr.IcebergContext icebergContext = new ScalarOperatorToIcebergExpr.IcebergContext(
                icebergTable.getNativeTable().schema().asStruct());
        Expression icebergPredicate = new ScalarOperatorToIcebergExpr().convert(scalarOperators, icebergContext);
        RemoteFileInfoSource baseSource = buildRemoteInfoSource(icebergTable, icebergPredicate, snapshotId.get());

        IcebergTableMORParams tableFullMORParams = param.getTableFullMORParams();
        if (tableFullMORParams.isEmpty()) {
            return baseSource;
        } else {
            // build remote file info source for table with equality delete files.
            String dbName = icebergTable.getCatalogDBName();
            String tableName = icebergTable.getCatalogTableName();
            IcebergRemoteFileInfoSourceKey remoteFileInfoSourceKey = IcebergRemoteFileInfoSourceKey.of(
                    dbName, tableName, snapshotId.get(), param.getPredicate(), tableFullMORParams.getMORId(),
                    param.getMORParams());

            if (!remoteFileInfoSources.containsKey(remoteFileInfoSourceKey)) {
                IcebergRemoteSourceTrigger trigger = new IcebergRemoteSourceTrigger(baseSource, tableFullMORParams);
                // The tableFullMORParams we recorded are mainly used here. Scheduling of multiple
                // split scan nodes from one table scan node is one by one. And in the IcebergRemoteSourceTrigger,
                // multiple queues need to be filled according to different iceberg mor params when executing iceberg planing.
                // Therefore, here we initialize the remoteFileInfoSource of all iceberg mor params for the first time.
                for (IcebergMORParams morParams : tableFullMORParams.getMorParamsList()) {
                    IcebergRemoteFileInfoSourceKey key = IcebergRemoteFileInfoSourceKey.of(
                            dbName, tableName, snapshotId.get(), param.getPredicate(), tableFullMORParams.getMORId(), morParams);
                    Deque<RemoteFileInfo> remoteFileInfoDeque = trigger.getQueue(morParams);
                    remoteFileInfoSources.put(key, new QueueIcebergRemoteFileInfoSource(trigger, remoteFileInfoDeque));
                }
            }

            return remoteFileInfoSources.get(remoteFileInfoSourceKey);
        }
    }

    private RemoteFileInfoSource buildRemoteInfoSource(IcebergTable table,
                                                       Expression icebergPredicate,
                                                       Long snapshotId) {
        Iterator<FileScanTask> iterator =
                buildFileScanTaskIterator(table, icebergPredicate, snapshotId, ConnectContext.get(), false);
        return new RemoteFileInfoSource() {
            @Override
            public RemoteFileInfo getOutput() {
                return new IcebergRemoteFileInfo(iterator.next());
            }

            @Override
            public boolean hasMoreOutput() {
                return iterator.hasNext();
            }
        };
    }

    private Iterator<FileScanTask> buildFileScanTaskIterator(IcebergTable icebergTable,
                                                             Expression icebergPredicate,
                                                             Long snapshotId,
                                                             ConnectContext connectContext,
                                                             boolean enableCollectColumnStats) {
        String dbName = icebergTable.getCatalogDBName();
        String tableName = icebergTable.getCatalogTableName();

        org.apache.iceberg.Table nativeTbl = icebergTable.getNativeTable();
        traceIcebergMetricsConfig(nativeTbl);

        StarRocksIcebergTableScanContext scanContext = new StarRocksIcebergTableScanContext(
                catalogName, dbName, tableName, planMode(connectContext), connectContext);
        scanContext.setLocalParallelism(catalogProperties.getIcebergJobPlanningThreadNum());
        scanContext.setLocalPlanningMaxSlotSize(catalogProperties.getLocalPlanningMaxSlotBytes());

        TableScan scan = icebergCatalog.getTableScan(nativeTbl, scanContext)
                .useSnapshot(snapshotId)
                .metricsReporter(metricsReporter)
                .planWith(jobPlanningExecutor);

        if (enableCollectColumnStats) {
            scan = scan.includeColumnStats();
        }

        if (icebergPredicate.op() != Expression.Operation.TRUE) {
            scan = scan.filter(icebergPredicate);
        }

        TableScan tableScan = scan;
        return new Iterator<>() {
            CloseableIterable<FileScanTask> fileScanTaskIterable;
            CloseableIterator<FileScanTask> fileScanTaskIterator;
            boolean hasMore = true;

            @Override
            public boolean hasNext() {
                if (hasMore) {
                    ensureOpen();
                }
                return hasMore;
            }

            @Override
            public FileScanTask next() {
                ensureOpen();
                return fileScanTaskIterator.next();
            }

            private void ensureOpen() {
                if (fileScanTaskIterator == null) {
                    fileScanTaskIterable = TableScanUtil.splitFiles(tableScan.planFiles(), tableScan.targetSplitSize());
                    fileScanTaskIterator = fileScanTaskIterable.iterator();
                }
                if (!fileScanTaskIterator.hasNext()) {
                    try {
                        fileScanTaskIterable.close();
                        fileScanTaskIterator.close();
                    } catch (Exception e) {
                        // ignore
                    }
                    hasMore = false;
                }
            }
        };
    }

    public Set<DeleteFile> getDeleteFiles(IcebergTable icebergTable, Long snapshotId,
                                          ScalarOperator predicate, FileContent content) {
        String dbName = icebergTable.getCatalogDBName();
        String tableName = icebergTable.getCatalogTableName();
        org.apache.iceberg.Table table = icebergTable.getNativeTable();
        List<ScalarOperator> scalarOperators = Utils.extractConjuncts(predicate);
        ScalarOperatorToIcebergExpr.IcebergContext icebergContext = new ScalarOperatorToIcebergExpr.IcebergContext(
                table.schema().asStruct());
        Expression icebergPredicate = new ScalarOperatorToIcebergExpr().convert(scalarOperators, icebergContext);

        StarRocksIcebergTableScanContext scanContext = new StarRocksIcebergTableScanContext(
                catalogName, dbName, tableName, PlanMode.LOCAL, ConnectContext.get());

        TableScan scan = icebergCatalog.getTableScan(table, scanContext)
                .useSnapshot(snapshotId)
                .metricsReporter(new IcebergMetricsReporter())
                .planWith(jobPlanningExecutor);

        if (icebergPredicate.op() != Expression.Operation.TRUE) {
            scan = scan.filter(icebergPredicate);
        }

        return ((StarRocksIcebergTableScan) scan).getDeleteFiles(content);
    }

    /**
     * To optimize the MetricsModes of the Iceberg tables, it's necessary to display the columns MetricsMode in the
     * ICEBERG query profile.
     * <br>
     * None:
     * <p>
     * Under this mode, value_counts, null_value_counts, nan_value_counts, lower_bounds, upper_bounds
     * are not persisted.
     * </p>
     * Counts:
     * <p>
     * Under this mode, only value_counts, null_value_counts, nan_value_counts are persisted.
     * </p>
     * Truncate:
     * <p>
     * Under this mode, value_counts, null_value_counts, nan_value_counts and truncated lower_bounds,
     * upper_bounds are persisted.
     * </p>
     * Full:
     * <p>
     * Under this mode, value_counts, null_value_counts, nan_value_counts and full lower_bounds,
     * upper_bounds are persisted.
     * </p>
     */
    public static Map<String, MetricsModes.MetricsMode> traceIcebergMetricsConfig(org.apache.iceberg.Table table) {
        MetricsModes.MetricsMode defaultMode = MetricsModes.fromString(DEFAULT_WRITE_METRICS_MODE_DEFAULT);
        MetricsConfig metricsConf = MetricsConfig.forTable(table);
        Map<String, MetricsModes.MetricsMode> fieldToMetricsMode = Maps.newHashMap();
        for (Types.NestedField field : table.schema().columns()) {
            MetricsModes.MetricsMode mode = metricsConf.columnMode(field.name());
            // To reduce printing, only print specific metrics that are not in the
            // DEFAULT_WRITE_METRICS_MODE_DEFAULT: truncate(16) mode
            if (!mode.equals(defaultMode)) {
                fieldToMetricsMode.put(field.name(), mode);
            }
        }

        if (!fieldToMetricsMode.isEmpty()) {
            Tracers.record(Tracers.Module.EXTERNAL, "ICEBERG.MetricsConfig." + table + ".write_metrics_mode_default",
                    DEFAULT_WRITE_METRICS_MODE_DEFAULT);
            Tracers.record(Tracers.Module.EXTERNAL, "ICEBERG.MetricsConfig." + table + ".non-default.size",
                    String.valueOf(fieldToMetricsMode.size()));
            Tracers.record(Tracers.Module.EXTERNAL, "ICEBERG.MetricsConfig." + table + ".non-default.columns",
                    fieldToMetricsMode.toString());
        }
        return fieldToMetricsMode;
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit,
                                         TableVersionRange version) {
        if (!properties.enableGetTableStatsFromExternalMetadata()) {
            return StatisticsUtils.buildDefaultStatistics(columns.keySet());
        }
        IcebergTable icebergTable = (IcebergTable) table;
        long snapshotId;
        if (version.end().isPresent()) {
            snapshotId = version.end().get();
        } else {
            return StatisticsUtils.buildDefaultStatistics(columns.keySet());
        }

        PredicateSearchKey key = PredicateSearchKey.of(
                icebergTable.getCatalogDBName(), icebergTable.getCatalogTableName(), snapshotId, predicate);

        triggerIcebergPlanFilesIfNeeded(key, icebergTable, predicate, limit);

        if (!session.getSessionVariable().enableIcebergColumnStatistics()) {
            List<FileScanTask> icebergScanTasks = splitTasks.get(key);
            if (icebergScanTasks == null) {
                throw new StarRocksConnectorException("Missing iceberg split task for table:[{}.{}]. predicate:[{}]",
                        icebergTable.getCatalogDBName(), icebergTable.getCatalogTableName(), predicate);
            }
            try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.calculateCardinality" + key)) {
                return statisticProvider.getCardinalityStats(columns, icebergScanTasks);
            }
        } else {
            return statisticProvider.getTableStatistics(icebergTable, columns, session, predicate, version);
        }
    }

    private IcebergSplitScanTask buildIcebergSplitScanTask(
            FileScanTask fileScanTask, Expression icebergPredicate, PredicateSearchKey filter) {
        long offset = fileScanTask.start();
        long length = fileScanTask.length();
        DataFile dataFileWithoutStats = fileScanTask.file().copyWithoutStats();
        DeleteFile[] deleteFiles = fileScanTask.deletes().stream()
                .map(DeleteFile::copyWithoutStats)
                .toArray(DeleteFile[]::new);

        PartitionSpec taskSpec = fileScanTask.spec();
        Schema taskSchema = fileScanTask.spec().schema();
        String schemaString;
        String partitionString;
        FileScanTaskSchema schemaKey = new FileScanTaskSchema(filter.getDatabaseName(), filter.getTableName(),
                taskSchema.schemaId(), taskSpec.specId());
        Pair<String, String> schema = fileScanTaskSchemas.get(schemaKey);
        if (schema == null) {
            schemaString = SchemaParser.toJson(fileScanTask.spec().schema());
            partitionString = PartitionSpecParser.toJson(fileScanTask.spec());
            fileScanTaskSchemas.put(schemaKey, Pair.create(schemaString, partitionString));
        } else {
            schemaString = schema.first;
            partitionString = schema.second;
        }

        ResidualEvaluator residualEvaluator = ResidualEvaluator.of(taskSpec, icebergPredicate, true);

        BaseFileScanTask baseFileScanTask = new BaseFileScanTask(
                dataFileWithoutStats,
                deleteFiles,
                schemaString,
                partitionString,
                residualEvaluator);
        return new IcebergSplitScanTask(offset, length, baseFileScanTask);
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        if (isResourceMappingCatalog(catalogName)) {
            refreshTableWithResource(table);
        } else {
            IcebergTable icebergTable = (IcebergTable) table;
            String dbName = icebergTable.getCatalogDBName();
            String tableName = icebergTable.getCatalogTableName();
            tables.remove(TableIdentifier.of(dbName, tableName));
            try {
                icebergCatalog.refreshTable(dbName, tableName, jobPlanningExecutor);
            } catch (Exception e) {
                LOG.error("Failed to refresh table {}.{}.{}. invalidate cache", catalogName, dbName, tableName, e);
                icebergCatalog.invalidateCache(dbName, tableName);
            }
        }
    }

    private void refreshTableWithResource(Table table) {
        IcebergTable icebergTable = (IcebergTable) table;
        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
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
    public void finishSink(String dbName, String tableName, List<TSinkCommitInfo> commitInfos, String branch) {
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

        if (branch != null) {
            batchWrite.toBranch(branch);
        }

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
            asyncRefreshOthersFeMetadataCache(dbName, tableName);
        } catch (Exception e) {
            List<String> toDeleteFiles = dataFiles.stream()
                    .map(TIcebergDataFile::getPath)
                    .collect(Collectors.toList());
            icebergCatalog.deleteUncommittedDataFiles(toDeleteFiles);
            LOG.error("Failed to commit iceberg transaction on {}.{}", dbName, tableName, e);
            throw new StarRocksConnectorException(e.getMessage());
        } finally {
            // Do we really need that? because partition cache is associated with snapshotId
            icebergCatalog.invalidatePartitionCache(dbName, tableName);
        }
    }

    private void asyncRefreshOthersFeMetadataCache(String dbName, String tableName) {
        refreshOtherFeExecutor.execute(() -> {
            LOG.info("Start to refresh others fe iceberg metadata cache on {}.{}.{}", catalogName, dbName, tableName);
            try {
                GlobalStateMgr.getCurrentState().refreshOthersFeTable(
                        new TableName(catalogName, dbName, tableName), new ArrayList<>(), false);
            } catch (DdlException e) {
                LOG.error("Failed to refresh others fe iceberg metadata cache {}.{}.{}", catalogName, dbName, tableName, e);
                throw new StarRocksConnectorException(e.getMessage());
            }
            LOG.info("Finish to refresh others fe iceberg metadata cache on {}.{}.{}", catalogName, dbName, tableName);
        });
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
            // apply url decoding for string/fixed type
            if (sourceType.typeId() == Type.TypeID.STRING || sourceType.typeId() == Type.TypeID.FIXED) {
                parts[1] = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
            }

            if (parts[1].equals("null")) {
                data.set(i, null);
            } else {
                data.set(i, Conversions.fromPartitionString(sourceType, parts[1]));
            }
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

    private PlanMode planMode(ConnectContext connectContext) {
        if (connectContext == null) {
            return PlanMode.LOCAL;
        }

        if (connectContext.getSessionVariable() == null) {
            return PlanMode.LOCAL;
        }

        return PlanMode.fromName(connectContext.getSessionVariable().getPlanMode());
    }

    private boolean enableCollectColumnStatistics(ConnectContext connectContext) {
        if (connectContext == null) {
            return false;
        }

        if (connectContext.getSessionVariable() == null) {
            return false;
        }

        return connectContext.getSessionVariable().enableIcebergColumnStatistics();
    }

    @Override
    public void clear() {
        splitTasks.clear();
        databases.clear();
        tables.clear();
        scannedTables.clear();
        metricsReporter.clear();
    }

    interface BatchWrite {
        void addFile(DataFile file);

        void commit();

        void toBranch(String targetBranch);
    }

    static class Append implements BatchWrite {
        private AppendFiles append;

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

        @Override
        public void toBranch(String targetBranch) {
            append = append.toBranch(targetBranch);
        }
    }

    static class DynamicOverwrite implements BatchWrite {
        private ReplacePartitions replace;

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

        @Override
        public void toBranch(String targetBranch) {
            replace = replace.toBranch(targetBranch);
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

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return hdfsEnvironment.getCloudConfiguration();
    }

    private static class FileScanTaskSchema {
        private final String dbName;
        private final String tableName;
        private final int schemaId;
        private final int specId;

        public FileScanTaskSchema(String dbName, String tableName, int schemaId, int specId) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.schemaId = schemaId;
            this.specId = specId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FileScanTaskSchema that = (FileScanTaskSchema) o;
            return schemaId == that.schemaId && specId == that.specId &&
                    Objects.equals(dbName, that.dbName) && Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName, schemaId, specId);
        }
    }
}
