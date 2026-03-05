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
import com.starrocks.sql.common.DmlException;
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
import org.apache.iceberg.FileFormat;
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
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StarRocksIcebergTableScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
    private final Set<PredicateSearchKey> scannedTables = ConcurrentHashMap.newKeySet();
    private final Set<PredicateSearchKey> preparedTables = ConcurrentHashMap.newKeySet();
    // Cache for manifest-level count optimization results (O(partitions) synthetic tasks).
    // Kept separate from splitTasks so the full file list remains available for non-count queries.
    private final Map<PredicateSearchKey, List<FileScanTask>> manifestCountCache = new ConcurrentHashMap<>();
    // Keys for which aggregateTasksByPartition() previously returned null (e.g. mixed
    // partitioned/unpartitioned files).  Avoids re-running the expensive planFiles() scan on
    // every query when the optimization is known to be inapplicable for a given snapshot key.
    private final Set<PredicateSearchKey> manifestCountOptFailedKeys = ConcurrentHashMap.newKeySet();

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
    public List<String> listDbNames(ConnectContext context) {
        return icebergCatalog.listAllDatabases(context);
    }

    @Override
    public void createDb(ConnectContext context, String dbName, Map<String, String> properties) throws AlreadyExistsException {
        if (dbExists(context, dbName)) {
            throw new AlreadyExistsException("Database Already Exists");
        }

        icebergCatalog.createDB(context, dbName, properties);
    }

    @Override
    public void dropDb(ConnectContext context, String dbName, boolean isForceDrop) throws MetaNotFoundException {
        context.getCurrentWarehouseName();

        if (listTableNames(context, dbName).size() != 0) {
            throw new StarRocksConnectorException("Database %s not empty", dbName);
        }

        icebergCatalog.dropDB(context, dbName);
        databases.remove(dbName);
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        if (databases.containsKey(dbName)) {
            return databases.get(dbName);
        }
        Database db;
        try {
            db = icebergCatalog.getDB(context, dbName);
        } catch (NoSuchNamespaceException e) {
            LOG.error("Database {} not found", dbName, e);
            return null;
        }

        databases.put(dbName, db);
        return db;
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return icebergCatalog.listTables(context, dbName);
    }

    @Override
    public boolean createTable(ConnectContext context, CreateTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        Schema schema = toIcebergApiSchema(stmt.getColumns());
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        if (partitionDesc != null && !(partitionDesc instanceof ListPartitionDesc)) {
            throw new DdlException("Only list partition is supported for iceberg table");
        }
        PartitionSpec partitionSpec = parsePartitionFields(schema, (ListPartitionDesc) partitionDesc);
        Map<String, String> properties = stmt.getProperties() == null ? new HashMap<>() : stmt.getProperties();
        String tableLocation = properties.get(LOCATION_PROPERTY);
        String comment = stmt.getComment();
        if (comment != null && !comment.isEmpty()) {
            properties.put(COMMENT, comment);
        }
        Map<String, String> createTableProperties = IcebergApiConverter.rebuildCreateTableProperties(properties);
        SortOrder sortOrder = IcebergApiConverter.toIcebergSortOrder(schema, stmt.getOrderByElements());

        return icebergCatalog.createTable(context, dbName, tableName, schema, partitionSpec, tableLocation,
                sortOrder, createTableProperties);
    }

    @Override
    public void createView(ConnectContext context, CreateViewStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String viewName = stmt.getTable();

        Database db = getDb(context, stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        if (getView(context, dbName, viewName) != null) {
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
        icebergCatalog.createView(context, catalogName, viewDefinition, stmt.isReplace());
    }

    @Override
    public void alterView(ConnectContext context, AlterViewStmt stmt) throws StarRocksException {
        String dbName = stmt.getDbName();
        String viewName = stmt.getTable();

        Database db = getDb(context, stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        if (getView(context, dbName, viewName) == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, dbName + "." + viewName);
        }

        ConnectorViewDefinition viewDefinition = ConnectorViewDefinition.fromAlterViewStmt(stmt);
        View currentView = icebergCatalog.getView(context, dbName, viewName);
        if (!stmt.getProperties().isEmpty() || stmt.isAlterDialect()) {
            icebergCatalog.alterView(context, currentView, viewDefinition);
        } else {
            throw new DdlException("ALTER VIEW <viewName> AS is not supported. Use CREATE OR REPLACE VIEW instead");
        }
    }

    @Override
    public void alterTable(ConnectContext context, AlterTableStmt stmt) throws StarRocksException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        org.apache.iceberg.Table table = icebergCatalog.getTable(context, dbName, tableName);

        if (table == null) {
            throw new StarRocksConnectorException(
                    "Failed to load iceberg table: " + stmt.getTbl().toString());
        }

        IcebergAlterTableExecutor executor = new IcebergAlterTableExecutor(stmt, table, icebergCatalog, context, hdfsEnvironment);
        executor.execute();

        synchronized (this) {
            tables.remove(TableIdentifier.of(dbName, tableName));
            try {
                icebergCatalog.refreshTable(dbName, tableName, context, jobPlanningExecutor);
            } catch (Exception exception) {
                LOG.error("Failed to refresh caching iceberg table.");
                icebergCatalog.invalidateCache(dbName, tableName);
            }
            asyncRefreshOthersFeMetadataCache(dbName, tableName);
        }
    }

    @Override
    public void dropTable(ConnectContext context, DropTableStmt stmt) {
        Table icebergTable = getTable(context, stmt.getDbName(), stmt.getTableName());

        if (icebergTable != null && icebergTable.isIcebergView()) {
            icebergCatalog.dropView(context, stmt.getDbName(), stmt.getTableName());
            return;
        }

        if (icebergTable == null) {
            return;
        }

        icebergCatalog.dropTable(context, stmt.getDbName(), stmt.getTableName(), stmt.isForceDrop());
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
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        TableIdentifier identifier = TableIdentifier.of(dbName, tblName);

        try {
            org.apache.iceberg.Table icebergTable;
            if (tables.containsKey(identifier)) {
                icebergTable = tables.get(identifier);
            } else {
                icebergTable = icebergCatalog.getTable(context, dbName, tblName);
            }

            IcebergCatalogType catalogType = icebergCatalog.getIcebergCatalogType();
            // Hive/Glue catalog table name is case-insensitive, normalize it to lower case
            if (catalogType == IcebergCatalogType.HIVE_CATALOG || catalogType == IcebergCatalogType.GLUE_CATALOG) {
                dbName = dbName.toLowerCase();
                tblName = tblName.toLowerCase();
            }
            Database db = getDb(context, dbName);
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
            return getView(context, dbName, tblName);
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

    public Table getView(ConnectContext context, String dbName, String viewName) {
        try {
            View icebergView = icebergCatalog.getView(context, dbName, viewName);
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
    public boolean tableExists(ConnectContext context, String dbName, String tblName) {
        return icebergCatalog.tableExists(context, dbName, tblName);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, ConnectorMetadatRequestContext requestContext) {
        IcebergCatalogType nativeType = icebergCatalog.getIcebergCatalogType();

        if (nativeType != HIVE_CATALOG && nativeType != REST_CATALOG && nativeType != GLUE_CATALOG) {
            throw new StarRocksConnectorException(
                    "Do not support get partitions from catalog type: " + nativeType);
        }

        ConnectContext ctx = ConnectContext.get();
        Table table = getTable(ctx != null ? ctx : new ConnectContext(), dbName, tblName);
        return icebergCatalog.listPartitionNames((IcebergTable) table, requestContext, jobPlanningExecutor);
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        TableVersionRange version = params.getTableVersionRange();
        long snapshotId = version.end().isPresent() ? version.end().get() : -1;
        IcebergTable icebergTable = (IcebergTable) table;
        String dbName = icebergTable.getCatalogDBName();
        String tableName = icebergTable.getCatalogTableName();
        if (params.isUseManifestCountOpt() && snapshotId != -1) {
            List<RemoteFileInfo> result = tryGetRemoteFilesViaManifestCount(
                    icebergTable, snapshotId, params.getPredicate());
            if (result != null) {
                LOG.info("getRemoteFiles [manifestCountOpt]: returning {} tasks for {}.{} snapshotId={}",
                        result.size(), dbName, tableName, snapshotId);
                return result;
            }
        }
        List<RemoteFileInfo> result = getRemoteFiles(icebergTable, snapshotId, params.getPredicate(), params.getLimit());
        LOG.info("getRemoteFiles [regular]: returning {} tasks for {}.{} snapshotId={} useManifestOpt={} limit={}",
                result.size(), dbName, tableName, snapshotId, params.isUseManifestCountOpt(), params.getLimit());
        return result;
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

    /**
     * Attempt to serve a getRemoteFiles request using manifest-level partition count aggregation.
     * Instead of returning O(files) scan tasks, returns one synthetic task per distinct partition
     * with a pre-aggregated record count. The BE honours the record_count in each scan range and
     * skips all Parquet I/O when canUseCountOpt is set (see HdfsScanner::can_use_count_optimization).
     *
     * Returns null if the optimization cannot be applied (e.g. table has delete files), in which
     * case the caller should fall back to the regular path.
     */
    private List<RemoteFileInfo> tryGetRemoteFilesViaManifestCount(IcebergTable table,
            long snapshotId, ScalarOperator predicate) {
        String dbName = table.getCatalogDBName();
        String tableName = table.getCatalogTableName();
        PredicateSearchKey key = PredicateSearchKey.of(dbName, tableName, snapshotId, predicate);

        // Return already-computed manifest-count result
        if (manifestCountCache.containsKey(key)) {
            List<FileScanTask> cached = manifestCountCache.get(key);
            LOG.info("tryGetRemoteFilesViaManifestCount: CACHE HIT for {}.{} snapshotId={} cachedSize={}",
                    dbName, tableName, snapshotId, cached.size());
            return cached.stream()
                    .map(IcebergRemoteFileInfo::new).collect(Collectors.toList());
        }

        // Short-circuit: a previous attempt for this snapshot key determined the optimization
        // is inapplicable (e.g. files with unpartitioned specs mixed with partitioned ones).
        // Skip the expensive planFiles() and return null immediately.
        if (manifestCountOptFailedKeys.contains(key)) {
            return null;
        }

        org.apache.iceberg.Table nativeTbl = table.getNativeTable();
        Snapshot snapshot = nativeTbl.snapshot(snapshotId);
        if (snapshot == null) {
            return null;
        }

        // If the snapshot has any delete files the manifest record_count may not reflect
        // actual live row counts; skip the optimization
        if (!snapshot.deleteManifests(nativeTbl.io()).isEmpty()) {
            return null;
        }

        // Convert the scan predicate to an Iceberg expression up front so we can:
        //   (a) bail out early if the predicate cannot be expressed as a partition filter, and
        //   (b) reuse the result in the cold-path planFiles() call below.
        //
        // ScalarOperatorToIcebergExpr returns Expressions.alwaysTrue() for any sub-expression
        // it cannot convert (e.g. function calls like date_trunc, upper, etc.).  If the whole
        // predicate collapses to alwaysTrue() but there WAS a predicate, Iceberg cannot prune
        // any files.  In that case record_count would cover rows that do not satisfy the
        // original filter, so we must not use the count optimisation.
        List<ScalarOperator> scalarOperators = Utils.extractConjuncts(predicate);
        ScalarOperatorToIcebergExpr.IcebergContext icebergContext =
                new ScalarOperatorToIcebergExpr.IcebergContext(nativeTbl.schema().asStruct());
        Expression icebergPredicate = new ScalarOperatorToIcebergExpr().convert(scalarOperators, icebergContext);

        if (predicate != null && icebergPredicate.op() == Expression.Operation.TRUE) {
            LOG.info("tryGetRemoteFilesViaManifestCount: predicate={} could not be converted to an " +
                    "Iceberg partition expression for {}.{}, skipping count optimisation",
                    predicate, dbName, tableName);
            return null;
        }

        // Read the manifest list — opens only the small manifest-list Avro file, not any
        // individual manifest or data file.  Cost is O(num_manifests), essentially free.
        List<ManifestFile> dataManifests = snapshot.dataManifests(nativeTbl.io());
        int currentSpecId = nativeTbl.spec().specId();

        // === Optimization A: pre-check manifest spec IDs ===
        // ManifestFile.partitionSpecId() is available from the manifest list with no extra IO.
        // If any manifest's spec is null or unpartitioned while the current spec is partitioned,
        // aggregateTasksByPartition() would bail out anyway — detect this cheaply and avoid
        // triggering the expensive planFiles() call.
        if (!nativeTbl.spec().isUnpartitioned()) {
            for (ManifestFile manifest : dataManifests) {
                if (manifest.partitionSpecId() != currentSpecId) {
                    PartitionSpec mSpec = nativeTbl.specs().get(manifest.partitionSpecId());
                    if (mSpec == null || mSpec.isUnpartitioned()) {
                        LOG.info("tryGetRemoteFilesViaManifestCount: manifest specId={} maps to a {} spec " +
                                "while current specId={} is partitioned for {}.{}, skipping",
                                manifest.partitionSpecId(),
                                mSpec == null ? "unknown" : "unpartitioned",
                                currentSpecId, dbName, tableName);
                        manifestCountOptFailedKeys.add(key);
                        return null;
                    }
                }
            }
        }

        // === Optimization B: aggregate from manifest row counts ===
        // When every manifest covers exactly one distinct partition value (lowerBound == upperBound
        // for all partition fields, no nulls), addedRowsCount + existingRowsCount in the manifest
        // is the exact live row count for that partition.  No planFiles() needed at all.
        // This is the common case for append-only daily-partitioned tables where each day's data
        // is written in a separate transaction (one manifest per partition).
        List<FileScanTask> manifestDerivedTasks =
                tryAggregateFromManifestRowCounts(nativeTbl, dataManifests, icebergPredicate);
        LOG.info("tryGetRemoteFilesViaManifestCount: pathB result={} numDataManifests={} for {}.{}",
                manifestDerivedTasks == null ? "null" : manifestDerivedTasks.size(),
                dataManifests.size(), dbName, tableName);
        if (manifestDerivedTasks != null && (!manifestDerivedTasks.isEmpty() || dataManifests.isEmpty())) {
            manifestCountCache.put(key, manifestDerivedTasks);
            return manifestDerivedTasks.stream()
                    .map(IcebergRemoteFileInfo::new).collect(Collectors.toList());
        }

        // Build or reuse the file-level task list.
        // When splitTasks already has the key we reuse those (possibly row-group-split) tasks for
        // the aggregation attempt.  When we plan from scratch we materialise planFiles() into a
        // list so the tasks remain available even after aggregateTasksByPartition consumes them.
        List<FileScanTask> fileScanTaskList;
        boolean fromFreshPlan;
        if (splitTasks.containsKey(key)) {
            // prepareMetadata already ran; reuse the in-memory list (avoid a second planFiles() call)
            fileScanTaskList = splitTasks.get(key);
            fromFreshPlan = false;
            LOG.info("tryGetRemoteFilesViaManifestCount: warm path for {}.{} snapshotId={} splitTasksSize={}",
                    dbName, tableName, snapshotId, fileScanTaskList.size());
        } else {
            // Cold path: plan files now without TableScanUtil.splitFiles — one task per file.
            // Materialise into a List so the tasks survive a failed aggregateTasksByPartition call.
            StarRocksIcebergTableScanContext scanContext = new StarRocksIcebergTableScanContext(
                    catalogName, dbName, tableName, planMode(ConnectContext.get()), ConnectContext.get());
            scanContext.setLocalParallelism(catalogProperties.getIcebergJobPlanningThreadNum());
            scanContext.setLocalPlanningMaxSlotSize(catalogProperties.getLocalPlanningMaxSlotBytes());

            TableScan scan = icebergCatalog.getTableScan(nativeTbl, scanContext)
                    .useSnapshot(snapshotId)
                    .metricsReporter(metricsReporter)
                    .planWith(jobPlanningExecutor);
            if (icebergPredicate.op() != Expression.Operation.TRUE) {
                scan = scan.filter(icebergPredicate);
            }
            try (CloseableIterable<FileScanTask> ownedIterable = scan.planFiles()) {
                fileScanTaskList = Lists.newArrayList(ownedIterable);
            } catch (Exception e) {
                LOG.warn("tryGetRemoteFilesViaManifestCount: planFiles failed for {}.{}: {}",
                        dbName, tableName, e.getMessage());
                manifestCountOptFailedKeys.add(key);
                return null;
            }
            fromFreshPlan = true;
        }

        List<FileScanTask> syntheticTasks = null;
        try {
            syntheticTasks = aggregateTasksByPartition(fileScanTaskList, nativeTbl);
        } catch (Exception e) {
            LOG.warn("Failed to aggregate tasks by partition for manifest count optimization on {}.{}: {}",
                    dbName, tableName, e.getMessage());
        }

        LOG.info("tryGetRemoteFilesViaManifestCount: aggregateTasksByPartition result={} for {}.{} " +
                "fromFreshPlan={} fileScanTaskListSize={}",
                syntheticTasks == null ? "null" : syntheticTasks.size(),
                dbName, tableName, fromFreshPlan, fileScanTaskList.size());

        if (syntheticTasks != null) {
            manifestCountCache.put(key, syntheticTasks);
            return syntheticTasks.stream().map(IcebergRemoteFileInfo::new).collect(Collectors.toList());
        }

        // Partition aggregation failed.  When we planned from scratch (no splitFiles), the
        // file-level list is still a valid fallback: 1 scan range per file instead of 1 per
        // row group (~8x cheaper).  The BE count optimisation reads record_count from scan-range
        // metadata and issues 0 bytes of S3 IO, so file-level granularity is perfectly correct.
        // Cache the list in manifestCountCache so subsequent count-opt queries skip re-planning.
        if (fromFreshPlan) {
            manifestCountCache.put(key, fileScanTaskList);
            return fileScanTaskList.stream().map(IcebergRemoteFileInfo::new).collect(Collectors.toList());
        }

        // We reused cached split tasks and aggregation still failed — mark the key as permanently
        // failed so subsequent queries bypass the planFiles() attempt entirely.
        manifestCountOptFailedKeys.add(key);
        return null;
    }

    /**
     * Attempts to build per-partition synthetic FileScanTasks directly from the manifest list,
     * avoiding planFiles() entirely.
     *
     * <p>Each ManifestFile in the manifest list carries per-partition-field summary bounds
     * (lowerBound / upperBound) and aggregate row counts (addedRowsCount + existingRowsCount).
     * When every manifest covers exactly one distinct partition value — i.e. lowerBound equals
     * upperBound for every partition field and neither is null — the manifest row count is the
     * exact live count for that partition.  Multiple manifests for the same partition are summed.
     *
     * <p>This is the common case for append-only daily-partitioned tables where each day's data
     * is written in a separate Iceberg transaction (one manifest per partition).  planFiles()
     * opens every manifest Avro file to iterate individual FileScanTask entries; this method
     * only needs the manifest list, which is already in memory.
     *
     * <p>Returns null if any manifest fails the single-partition or row-count-available check,
     * signalling the caller to fall back to planFiles().
     */
    private List<FileScanTask> tryAggregateFromManifestRowCounts(
            org.apache.iceberg.Table nativeTbl,
            List<ManifestFile> dataManifests,
            Expression icebergPredicate) {

        // Apply partition-level predicate pruning using PartitionFieldSummary bounds.
        // For alwaysTrue() (no predicate) this returns all manifests unchanged.
        // For a partition equality / range filter this eliminates manifests whose
        // partition bounds provably cannot contain matching rows.
        List<ManifestFile> filtered = IcebergApiConverter.filterManifests(
                dataManifests, nativeTbl, icebergPredicate);

        // filterManifests removes manifests where hasAddedFiles() and hasExistingFiles() are
        // both false.  This can happen when manifest-list statistics (added_files_count,
        // existing_files_count) are null — e.g. manifests written by older Iceberg writers or
        // third-party engines that omit these optional fields.  If filtering removed all
        // manifests but the input was non-empty, the counts would be zero (incorrect).
        // Fall back to planFiles() which reads the actual manifest entries.
        if (filtered.isEmpty() && !dataManifests.isEmpty()) {
            LOG.info("tryAggregateFromManifestRowCounts: filterManifests removed all {} manifests " +
                    "for table {}, falling back to planFiles()",
                    dataManifests.size(), nativeTbl.location());
            return null;
        }

        Map<String, Long> countByKey = new LinkedHashMap<>();
        Map<String, StructLike> partitionByKey = new LinkedHashMap<>();
        Map<String, Integer> specIdByKey = new LinkedHashMap<>();

        for (ManifestFile manifest : filtered) {
            // Row counts must be present — older Iceberg writers may omit them.
            Long addedRows = manifest.addedRowsCount();
            Long existingRows = manifest.existingRowsCount();
            Long deletedRows = manifest.deletedRowsCount();
            if (addedRows == null && existingRows == null) {
                return null;
            }
            // If any file entries in this manifest have been superseded (REPLACE/OVERWRITE
            // operations produce DELETED-status entries), deletedRowsCount() reflects rows in
            // those deleted files.  addedRows + existingRows would overcount.
            // Fall back to planFiles() which returns only live files.
            if (deletedRows != null && deletedRows > 0) {
                return null;
            }
            long rowCount = (addedRows != null ? addedRows : 0L)
                    + (existingRows != null ? existingRows : 0L);

            // Fetch the partition spec before checking summaries so we can special-case
            // unpartitioned tables (empty summaries list).
            int mSpecId = manifest.partitionSpecId();
            PartitionSpec mSpec = nativeTbl.specs().get(mSpecId);
            if (mSpec == null) {
                return null;
            }

            List<ManifestFile.PartitionFieldSummary> summaries = manifest.partitions();

            if (!mSpec.isUnpartitioned()) {
                // For partitioned manifests every field must cover exactly one distinct value
                // so we can derive an exact per-partition count from the manifest row count.
                if (summaries == null || summaries.isEmpty() || mSpec.fields().size() != summaries.size()) {
                    return null;
                }
                for (ManifestFile.PartitionFieldSummary summary : summaries) {
                    if (summary.containsNull()
                            || summary.lowerBound() == null || summary.upperBound() == null
                            || !summary.lowerBound().equals(summary.upperBound())) {
                        return null;
                    }
                }
            }

            // Decode the single partition value for each field from lowerBound.
            // For unpartitioned tables summaries is empty; we produce an empty-key partition.
            int numFields = summaries == null ? 0 : summaries.size();
            Types.StructType partitionType = mSpec.partitionType();
            PartitionData partitionData = new PartitionData(numFields);
            for (int i = 0; i < numFields; i++) {
                Type nestedType = partitionType.fields().get(i).type();
                Object value = Conversions.fromByteBuffer(nestedType, summaries.get(i).lowerBound());
                partitionData.set(i, value);
            }

            String pKey = makePartitionKey(mSpecId, mSpec, partitionData);
            countByKey.merge(pKey, rowCount, Long::sum);
            partitionByKey.putIfAbsent(pKey, partitionData);
            specIdByKey.putIfAbsent(pKey, mSpecId);
        }

        // Build one synthetic FileScanTask per distinct (specId, partition) with the
        // aggregated row count.  Empty result is valid (predicate matched no partitions).
        String tableLocation = nativeTbl.location();
        List<FileScanTask> result = new ArrayList<>(countByKey.size());
        for (Map.Entry<String, Long> entry : countByKey.entrySet()) {
            String pKey = entry.getKey();
            long count = entry.getValue();
            StructLike partData = partitionByKey.get(pKey);
            int specId = specIdByKey.get(pKey);
            PartitionSpec spec = nativeTbl.specs().get(specId);

            DataFile syntheticFile = DataFiles.builder(spec)
                    .withPath(tableLocation + "/count-opt-" + pKey)
                    .withFileSizeInBytes(0)
                    .withFormat(FileFormat.PARQUET)
                    .withPartition(partData)
                    .withRecordCount(count)
                    .build();

            result.add(new SyntheticCountFileScanTask(syntheticFile, spec));
        }
        return result;
    }

    /**
     * Aggregates an iterable of file-level scan tasks into one synthetic task per distinct partition.
     * Each synthetic task carries a pre-summed record_count and an empty deletes list so the BE
     * can short-circuit with canUseCountOpt without reading any data files.
     */
    private List<FileScanTask> aggregateTasksByPartition(Iterable<FileScanTask> tasks,
            org.apache.iceberg.Table nativeTbl) {
        // specId + "|" + per-field values → aggregated count / representative partition StructLike
        Map<String, Long> countByKey = new LinkedHashMap<>();
        Map<String, StructLike> partitionByKey = new LinkedHashMap<>();
        Map<String, Integer> specIdByKey = new LinkedHashMap<>();

        int currentSpecId = nativeTbl.spec().specId();
        boolean currentSpecIsPartitioned = !nativeTbl.spec().isUnpartitioned();

        for (FileScanTask task : tasks) {
            int specId = task.file().specId();

            // If the file was written under a different spec than the current table spec, only
            // bail out when the file's *own* spec is absent from the specs map or is unpartitioned.
            // In the unpartitioned case task.file().partition() returns an empty struct, so every
            // such file would land in the null-partition bucket — incorrect for a partitioned table.
            //
            // When the file's spec IS partitioned (different spec ID but still has partition fields,
            // e.g. Hudi tables synced via XTable where spec IDs may differ across file batches),
            // task.file().partition() still carries valid partition data under that spec's layout.
            // We aggregate those files using their own spec's partition key; the resulting synthetic
            // tasks are merged correctly by the BE's GROUP BY aggregation.
            if (currentSpecIsPartitioned && specId != currentSpecId) {
                PartitionSpec fileSpec = nativeTbl.specs().get(specId);
                if (fileSpec == null || fileSpec.isUnpartitioned()) {
                    LOG.info("aggregateTasksByPartition: file specId={} maps to a {} spec " +
                            "while current specId={} is partitioned for table {}, " +
                            "skipping manifest count optimization",
                            specId, fileSpec == null ? "unknown" : "unpartitioned",
                            currentSpecId, nativeTbl.location());
                    return null;
                }
                // File spec is partitioned — partition data is valid, continue with its own spec.
            }

            PartitionSpec spec = nativeTbl.specs().get(specId);
            if (spec == null) {
                // Unknown spec — cannot build a reliable partition key.
                LOG.warn("aggregateTasksByPartition: unknown specId={} for table {}, skipping optimization",
                        specId, nativeTbl.location());
                return null;
            }
            StructLike partition = task.file().partition();
            String key = makePartitionKey(specId, spec, partition);
            countByKey.merge(key, task.file().recordCount(), Long::sum);
            partitionByKey.putIfAbsent(key, partition);
            specIdByKey.putIfAbsent(key, specId);
        }

        String tableLocation = nativeTbl.location();
        List<FileScanTask> result = new ArrayList<>(countByKey.size());
        for (Map.Entry<String, Long> entry : countByKey.entrySet()) {
            String key = entry.getKey();
            long count = entry.getValue();
            StructLike partitionData = partitionByKey.get(key);
            int specId = specIdByKey.get(key);
            PartitionSpec spec = nativeTbl.specs().get(specId);

            DataFile syntheticFile = DataFiles.builder(spec)
                    .withPath(tableLocation + "/count-opt-" + Math.abs(key.hashCode()))
                    .withFileSizeInBytes(0)
                    .withFormat(FileFormat.PARQUET)
                    .withPartition(partitionData)
                    .withRecordCount(count)
                    .build();

            result.add(new SyntheticCountFileScanTask(syntheticFile, spec));
        }
        return result;
    }

    /** Builds a stable, collision-free string key for a (specId, partition) pair.
     *
     * Uses length-prefixed encoding for each field value so that string partition
     * columns containing the separator characters cannot produce false collisions.
     * Format: {@code specId|len:value len:value ...}  where null fields use {@code -1:}.
     */
    private static String makePartitionKey(int specId, PartitionSpec spec, StructLike partition) {
        StringBuilder sb = new StringBuilder().append(specId).append('|');
        for (int i = 0; i < spec.fields().size(); i++) {
            Object value = partition.get(i, Object.class);
            if (value == null) {
                sb.append("-1:");
            } else {
                String s = value.toString();
                sb.append(s.length()).append(':').append(s);
            }
        }
        return sb.toString();
    }

    /**
     * A minimal FileScanTask implementation representing an entire partition's record count.
     * Has no delete files so the BE's can_use_file_record_count check passes, enabling the
     * count optimization short-circuit in HdfsScanner::get_next().
     */
    private static final class SyntheticCountFileScanTask implements FileScanTask {
        private final DataFile file;
        private final PartitionSpec spec;

        private SyntheticCountFileScanTask(DataFile file, PartitionSpec spec) {
            this.file = file;
            this.spec = spec;
        }

        @Override
        public DataFile file() {
            return file;
        }

        @Override
        public List<DeleteFile> deletes() {
            return Collections.emptyList();
        }

        @Override
        public PartitionSpec spec() {
            return spec;
        }

        @Override
        public long start() {
            return 0;
        }

        @Override
        public long length() {
            return 0;
        }

        @Override
        public Expression residual() {
            return Expressions.alwaysTrue();
        }

        @Override
        public Iterable<FileScanTask> split(long splitSize) {
            return Lists.newArrayList(this);
        }
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
        IcebergTable icebergTable = (IcebergTable) getTable(ConnectContext.get(), dbName, tableName);
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
        // For a limited scan the resulting file list is a subset of the full set.  We must NOT
        // serve that truncated list to a subsequent unlimited query keyed on the same
        // (table, snapshot, predicate), so we bypass the scannedTables cache entirely for
        // limited scans and let every call go through to collectTableStatisticsAndCacheIcebergSplit
        // which will re-plan (and may terminate early again).
        if (limit >= 0 || !scannedTables.contains(key)) {
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
        // Partition enumeration must visit all matching files regardless of any query-level LIMIT,
        // so force unlimited planning here.
        triggerIcebergPlanFilesIfNeeded(key, icebergTable, predicate, -1);

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

        // Early termination: we can stop planning files once the accumulated record count reaches the LIMIT,
        // but only when the planFiles() result is EXACT (no rows are over- or under-counted).
        //
        // Safety condition:
        //   1. No predicate (alwaysTrue): every row in every file is returned → record_count is exact.
        //   2. Predicate covers only identity-partitioned columns: Iceberg's partition pruning
        //      selects files where ALL rows satisfy the predicate → record_count is exact.
        //
        // For any other predicate (non-partition columns, non-identity transforms) the
        // planFiles() result is inclusive (some files may contain non-matching rows), so
        // record_count is an upper bound and early termination could yield too few rows.
        //
        // Additionally we only count rows from files that have no associated delete files;
        // files with deletes may have a lower effective row count than record_count reports.
        boolean canTerminateEarly = limit >= 0 && isPredicatePartitionEnforced(nativeTbl, predicate);
        long outputRowsLowerBound = 0;

        List<FileScanTask> icebergScanTasks = Lists.newArrayList();

        while (iterator.hasNext()) {
            if (canTerminateEarly && outputRowsLowerBound >= limit) {
                break;
            }
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
            // Accumulate row count only for files without deletes; files with deletes have a
            // lower effective count than record_count reports, so we skip them to stay conservative
            // (we may plan extra files, but we will never under-plan).
            if (canTerminateEarly && scanTask.deletes().isEmpty()) {
                outputRowsLowerBound += scanTask.file().recordCount();
            }
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
        LOG.info("collectTableStatisticsAndCacheIcebergSplit: stored {} tasks for {}.{} snapshotId={} " +
                "limit={} canTerminateEarly={}",
                icebergScanTasks.size(), dbName, tableName, snapshotId, limit, canTerminateEarly);
        // Only mark as fully scanned for unlimited queries (limit == -1).
        // limit = 0 means SQL LIMIT 0 (zero rows), and limit > 0 means a real LIMIT n:
        // both produce a truncated (or empty) file list that must not be served to
        // subsequent unlimited queries sharing the same (table, snapshot, predicate) key.
        if (limit < 0) {
            scannedTables.add(key);
        }
    }

    /**
     * Returns true when planFiles() with the given predicate returns EXACT results — i.e. every
     * file in the result contains only rows that satisfy the predicate, so file.recordCount() is a
     * reliable lower bound on the number of output rows.
     *
     * This is true in two cases:
     *   1. No predicate (null) — all rows in every file are included.
     *   2. The predicate references ONLY identity-partitioned columns across ALL historical specs —
     *      Iceberg's partition pruning selects files from exactly the matching partitions, and
     *      within an identity-partitioned file every row satisfies the partition predicate.
     *
     * For any non-partition column predicate, planFiles() applies inclusive file-level stats
     * pruning (lower/upper bounds), so a returned file may contain non-matching rows, making
     * record_count an over-estimate.
     *
     * Historical specs are checked (not just the current one) because a table that evolved from
     * day(ts) to identity(ts) still has old files where partition values are epoch-day integers,
     * not raw timestamps; early termination on their record_count would be unsafe.
     *
     * Note: this check is type-agnostic — it evaluates the partition transform, not the column
     * data type.  A string identity partition (e.g. identity(region)) is just as safe as a
     * numeric one: every data file in an identity partition contains exactly one partition value,
     * so file.recordCount() is exact regardless of whether the type is integer, date, or string.
     * Non-identity string transforms such as truncate(N, col) or bucket(N, col) produce files
     * whose rows may span many distinct column values, so those correctly return false here.
     */
    private boolean isPredicatePartitionEnforced(org.apache.iceberg.Table nativeTbl, ScalarOperator predicate) {
        if (predicate == null) {
            return true;
        }
        List<ColumnRefOperator> colRefs = Utils.extractColumnRef(predicate);
        if (colRefs.isEmpty()) {
            // Constant predicate (no column references) — trivially safe.
            return true;
        }
        Set<String> colNames = colRefs.stream()
                .map(ColumnRefOperator::getName)
                .collect(Collectors.toSet());
        // Every historical spec must use identity (or void, for dropped columns) for every
        // column referenced by the predicate.  Any non-identity transform means files under
        // that spec may have rows that do not match the predicate.
        for (PartitionSpec spec : nativeTbl.specs().values()) {
            for (PartitionField field : spec.fields()) {
                if (field.transform().isVoid()) {
                    continue; // dropped partition column — never contributes data
                }
                String colName = nativeTbl.schema().findColumnName(field.sourceId());
                if (colNames.contains(colName) && !field.transform().isIdentity()) {
                    return false;
                }
            }
        }
        // Also verify the predicate columns are actually partition columns in the current spec.
        Set<String> currentIdentityPartitionCols = nativeTbl.spec().fields().stream()
                .filter(f -> f.transform().isIdentity())
                .map(f -> nativeTbl.schema().findColumnName(f.sourceId()))
                .collect(Collectors.toSet());
        return colRefs.stream().allMatch(ref -> currentIdentityPartitionCols.contains(ref.getName()));
    }

    @Override
    public RemoteFileInfoSource getRemoteFilesAsync(Table table, GetRemoteFilesParams params) {
        IcebergGetRemoteFilesParams param = params.cast();
        Optional<Long> snapshotId = param.getTableVersionRange().end();
        if (snapshotId.isEmpty()) {
            return RemoteFileInfoDefaultSource.EMPTY;
        }

        IcebergTable icebergTable = (IcebergTable) table;

        String dbName = table.getCatalogDBName();
        String tableName = table.getCatalogTableName();
        PredicateSearchKey predicateSearchKey = PredicateSearchKey.of(dbName, tableName, snapshotId.get(), param.getPredicate());
        RemoteFileInfoSource baseSource;
        // Only reuse a cached file list when scannedTables confirms it was produced by a full
        // (unlimited) scan.  A limited-query result in splitTasks is a truncated subset and must
        // not be served here.
        if (splitTasks.containsKey(predicateSearchKey) && scannedTables.contains(predicateSearchKey)) {
            baseSource = buildRemoteInfoSource(splitTasks.get(predicateSearchKey));
        } else {
            List<ScalarOperator> scalarOperators = Utils.extractConjuncts(params.getPredicate());
            ScalarOperatorToIcebergExpr.IcebergContext icebergContext = new ScalarOperatorToIcebergExpr.IcebergContext(
                    icebergTable.getNativeTable().schema().asStruct());
            Expression icebergPredicate = new ScalarOperatorToIcebergExpr().convert(scalarOperators, icebergContext);
            baseSource = buildRemoteInfoSource(icebergTable, icebergPredicate, snapshotId.get(), params);
        }

        IcebergTableMORParams tableFullMORParams = param.getTableFullMORParams();
        if (tableFullMORParams.isEmpty()) {
            return baseSource;
        } else {
            // build remote file info source for table with equality delete files.
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
                                                       Long snapshotId,
                                                       GetRemoteFilesParams params) {
        Iterator<FileScanTask> iterator =
                buildFileScanTaskIterator(table, icebergPredicate, snapshotId, ConnectContext.get(),
                        params.isEnableColumnStats());
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

    private RemoteFileInfoSource buildRemoteInfoSource(List<FileScanTask> tasks) {
        Iterator<FileScanTask> iterator = tasks.iterator();
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
            ConnectContext ctx = ConnectContext.get();
            if (ctx == null && icebergCatalog.getIcebergCatalogType() == REST_CATALOG) {
                // No user context on this background thread. For JWT-protected REST catalogs
                // we cannot authenticate, so skip the refresh and let the existing cache
                // entry remain valid until its TTL expires.
                LOG.debug("No ConnectContext on background thread for JWT REST catalog {}.{}.{}, skipping refresh",
                        catalogName, dbName, tableName);
                return;
            }
            try {
                icebergCatalog.refreshTable(dbName, tableName, ctx, jobPlanningExecutor);
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
    public void finishSink(ConnectContext context, String dbName, String tableName,
                           List<TSinkCommitInfo> commitInfos, String branch) {
        finishSink(context, dbName, tableName, commitInfos, branch, null);
    }

    @Override
    public void finishSink(ConnectContext context, String dbName, String tableName,
                           List<TSinkCommitInfo> commitInfos, String branch, Object extra) {
        boolean isOverwrite = false;
        boolean isRewrite = false;
        if (!commitInfos.isEmpty()) {
            TSinkCommitInfo sinkCommitInfo = commitInfos.get(0);
            if (sinkCommitInfo.isSetIs_overwrite()) {
                isOverwrite = sinkCommitInfo.is_overwrite;
            } else if (sinkCommitInfo.isSetIs_rewrite()) {
                isRewrite = sinkCommitInfo.is_rewrite;
            }
        }

        List<TIcebergDataFile> dataFiles = commitInfos.stream()
                .map(TSinkCommitInfo::getIceberg_data_file).collect(Collectors.toList());

        ConnectContext connectContext = context != null ? context : new ConnectContext();
        IcebergTable table = (IcebergTable) getTable(connectContext, dbName, tableName);
        org.apache.iceberg.Table nativeTbl = table.getNativeTable();
        Transaction transaction = nativeTbl.newTransaction();
        BatchWrite batchWrite = getBatchWrite(transaction, isOverwrite, isRewrite);

        if (branch != null) {
            batchWrite.toBranch(branch);
        }

        PartitionSpec partitionSpec = nativeTbl.spec();
        for (TIcebergDataFile dataFile : dataFiles) {
            Metrics metrics = IcebergApiConverter.buildDataFileMetrics(dataFile, nativeTbl);
            DataFiles.Builder builder =
                    DataFiles.builder(partitionSpec)
                            .withMetrics(metrics)
                            .withPath(dataFile.path)
                            .withFormat(dataFile.format)
                            .withRecordCount(dataFile.record_count)
                            .withFileSizeInBytes(dataFile.file_size_in_bytes)
                            .withSplitOffsets(dataFile.split_offsets);
            String nullFingerprint = "";
            if (!dataFile.isSetPartition_null_fingerprint()) {
                nullFingerprint = "0".repeat(partitionSpec.fields().size());
            } else {
                nullFingerprint = dataFile.getPartition_null_fingerprint();
            }
            if (partitionSpec.isPartitioned()) {
                String relativePartitionLocation = getIcebergRelativePartitionPath(
                        nativeTbl.location(), dataFile.partition_path);
                PartitionData partitionData = partitionDataFromPath(
                        relativePartitionLocation, nullFingerprint, partitionSpec, nativeTbl);
                builder.withPartition(partitionData);
                // builder.withPartitionPath(relativePartitionLocation);
            }
            batchWrite.addFile(builder.build());
        }

        if (isRewrite && extra != null) {
            ((IcebergSinkExtra) extra).getScannedDataFiles().forEach(batchWrite::deleteFile);
            ((IcebergSinkExtra) extra).getAppliedDeleteFiles().forEach(batchWrite::deleteFile);
            ((RewriteData) batchWrite).setSnapshotId(nativeTbl.currentSnapshot().snapshotId());
        }

        try {
            batchWrite.commit();
            transaction.commitTransaction();
            asyncRefreshOthersFeMetadataCache(dbName, tableName);
        } catch (Exception e) {
            if (!(e instanceof CommitStateUnknownException)) {
                List<String> toDeleteFiles = dataFiles.stream()
                        .map(TIcebergDataFile::getPath)
                        .collect(Collectors.toList());
                icebergCatalog.deleteUncommittedDataFiles(toDeleteFiles);
            }
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

    public BatchWrite getBatchWrite(Transaction transaction, boolean isOverwrite, boolean isRewrite) {
        if (isRewrite) {
            return new RewriteData(transaction);
        } else if (isOverwrite) {
            return new DynamicOverwrite(transaction);
        }
        return new Append(transaction);
    }

    public PartitionData partitionDataFromPath(String relativePartitionPath,
                                               String partitionNullFingerprint, PartitionSpec spec,
                                               org.apache.iceberg.Table table) {
        PartitionData data = new PartitionData(spec.fields().size());
        String[] partitions = relativePartitionPath.split("/", -1);
        List<PartitionField> partitionFields = spec.fields();
        if (partitions.length != partitionNullFingerprint.length()) {
            throw new InternalError("Invalid partition and fingerprint size, partition:" + relativePartitionPath +
                    " partition size:" + String.valueOf(partitions.length) + " fingerprint:" + partitionNullFingerprint);
        }
        for (int i = 0; i < partitions.length; i++) {
            PartitionField field = partitionFields.get(i);
            String[] parts = partitions[i].split("=", 2);
            Preconditions.checkArgument(parts.length == 2 && parts[0] != null &&
                    field.name().equals(parts[0]), "Invalid partition: %s", partitions[i]);
            org.apache.iceberg.types.Type resultType = spec.partitionType().fields().get(i).type();
            // org.apache.iceberg.types.Type sourceType = table.schema().findType(field.sourceId());
            // NOTICE:
            // The behavior here should match the make_partition_path method in be,
            // and revert the String path value to the origin value and type of transform expr for the metastore.
            // otherwise, if we use the api of iceberg to filter the scan files, the result may be incorrect!
            if (partitionNullFingerprint.charAt(i) == '0') { //'0' means not null, '1' means null
                // apply date decoding for date type
                String transform = field.transform().toString();
                if (transform.equals("year") || transform.equals("month")
                        || transform.equals("day") || transform.equals("hour")) {
                    Integer year = org.apache.iceberg.util.DateTimeUtil.EPOCH.getYear();
                    Integer month = org.apache.iceberg.util.DateTimeUtil.EPOCH.getMonthValue();
                    Integer day = org.apache.iceberg.util.DateTimeUtil.EPOCH.getDayOfMonth();
                    Integer hour = org.apache.iceberg.util.DateTimeUtil.EPOCH.getHour();
                    String[] dateParts = parts[1].split("-");
                    if (dateParts.length > 0) {
                        year = Integer.parseInt(dateParts[0]);
                    }
                    if (dateParts.length > 1) {
                        month = Integer.parseInt(dateParts[1]);
                    }
                    if (dateParts.length > 2) {
                        day = Integer.parseInt(dateParts[2]);
                    }
                    if (dateParts.length > 3) {
                        hour = Integer.parseInt(dateParts[3]);
                    }
                    LocalDateTime target = LocalDateTime.of(year, month, day, hour, 0);
                    if (transform.equals("year")) {
                        //iceberg stores the result of transform as metadata.
                        parts[1] = String.valueOf(
                                ChronoUnit.YEARS.between(org.apache.iceberg.util.DateTimeUtil.EPOCH_DAY, target));
                    } else if (transform.equals("month")) {
                        parts[1] = String.valueOf(
                                ChronoUnit.MONTHS.between(org.apache.iceberg.util.DateTimeUtil.EPOCH_DAY, target));
                    } else if (transform.equals("day")) {
                        //The reuslt of day transform is a date type.
                        //It is diffrent from other date transform exprs, however other's result is a integer.
                        //do nothing
                    } else if (transform.equals("hour")) {
                        parts[1] = String.valueOf(
                                ChronoUnit.HOURS.between(org.apache.iceberg.util.DateTimeUtil.EPOCH_DAY.atTime(0, 0), target));
                    }
                } else if (transform.startsWith("truncate")) {
                    //the result type of truncate is the same as the truncate column
                    if (parts[1].length() == 0) {
                        //do nothing
                    } else if (resultType.typeId() == Type.TypeID.STRING || resultType.typeId() == Type.TypeID.FIXED) {
                        parts[1] = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                    } else if (resultType.typeId() == Type.TypeID.BINARY) {
                        parts[1] = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                        //Do not convert the byte array to utf-8, because some byte is not valid in utf-8.
                        //like 0xE6 is not valid in utf8. If the convert failed, utf-8 will transfer the byte to 0xFFFD as default
                        //we should just read and store the byte in latin, and thus not change the byte array value.
                        parts[1] = new String(Base64.getDecoder().decode(parts[1]), StandardCharsets.ISO_8859_1);
                    }
                } else if (transform.startsWith("bucket")) {
                    //the result type of bucket is integer.
                    //do nothing
                } else if (transform.equals("identity")) {
                    if (parts[1].length() == 0) {
                        //do nothing
                    } else if (resultType.typeId() == Type.TypeID.STRING || resultType.typeId() == Type.TypeID.FIXED) {
                        parts[1] = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                    } else if (resultType.typeId() == Type.TypeID.BINARY) {
                        parts[1] = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                        parts[1] = new String(Base64.getDecoder().decode(parts[1]), StandardCharsets.ISO_8859_1);
                    } else if (resultType.typeId() == Type.TypeID.TIMESTAMP) {
                        parts[1] = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                        parts[1] = parts[1].replace(' ', 'T');
                    }
                } else {
                    throw new DmlException("Unsupported partition transform: %s", transform);
                }
            }

            if (partitionNullFingerprint.charAt(i) == '1') {
                data.set(i, null);
            } else if (resultType.typeId() == Type.TypeID.BINARY) {
                data.set(i, parts[1].getBytes(StandardCharsets.ISO_8859_1));
            } else if (resultType.typeId() == Type.TypeID.TIMESTAMP) {
                data.set(i, Literal.of(parts[1]).to(Types.TimestampType.withoutZone()).value());
            } else {
                data.set(i, Conversions.fromPartitionString(resultType, parts[1]));
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
        manifestCountCache.clear();
        databases.clear();
        tables.clear();
        scannedTables.clear();
        manifestCountOptFailedKeys.clear();
        metricsReporter.clear();
    }

    public interface BatchWrite {
        void addFile(DataFile file);

        void deleteFile(DeleteFile file);

        void deleteFile(DataFile file);

        void commit();

        void toBranch(String targetBranch);
    }

    public static class Append implements BatchWrite {
        private AppendFiles append;

        public Append(Transaction txn) {
            append = txn.newAppend();
        }

        @Override
        public void addFile(DataFile file) {
            append.appendFile(file);
        }

        @Override
        public void deleteFile(DeleteFile file) {
            //not implement
        }

        @Override
        public void deleteFile(DataFile file) {
            //not implement
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

    public static class IcebergSinkExtra {
        private final Set<DataFile> scannedDataFiles;
        private final Set<DeleteFile> appliedDeleteFiles;

        public IcebergSinkExtra() {
            this.scannedDataFiles = new HashSet<>();
            this.appliedDeleteFiles = new HashSet<>();
        }

        public void addScannedDataFiles(Set<DataFile> o) {
            scannedDataFiles.addAll(o);
        }

        public void addAppliedDeleteFiles(Set<DeleteFile> o) {
            appliedDeleteFiles.addAll(o);
        }

        public Set<DataFile> getScannedDataFiles() {
            return scannedDataFiles;
        }

        public Set<DeleteFile> getAppliedDeleteFiles() {
            return appliedDeleteFiles;
        }
    }

    public static class DynamicOverwrite implements BatchWrite {
        private ReplacePartitions replace;

        public DynamicOverwrite(Transaction txn) {
            replace = txn.newReplacePartitions();
        }

        @Override
        public void addFile(DataFile file) {
            replace.addFile(file);
        }

        @Override
        public void deleteFile(DeleteFile file) {
            //not implement
        }

        @Override
        public void deleteFile(DataFile file) {
            //not implement
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

    public static class RewriteData implements BatchWrite {
        private RewriteFiles rewriteFiles;

        public RewriteData(Transaction txn) {
            rewriteFiles = txn.newRewrite();
        }

        @Override
        public void addFile(DataFile file) {
            rewriteFiles.addFile(file);
        }

        @Override
        public void deleteFile(DeleteFile file) {
            rewriteFiles.deleteFile(file);
        }

        @Override
        public void deleteFile(DataFile file) {
            rewriteFiles.deleteFile(file);
        }

        @Override
        public void commit() {
            rewriteFiles.commit();
        }

        @Override
        public void toBranch(String targetBranch) {
            rewriteFiles = rewriteFiles.toBranch(targetBranch);
        }

        public void setSnapshotId(long snapshotId) {
            rewriteFiles.validateFromSnapshot(snapshotId);
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
            Object value = values[pos];
            if (javaClass == ByteBuffer.class && value instanceof byte[]) {
                value = ByteBuffer.wrap((byte[]) value);
            }
            return javaClass.cast(value);
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