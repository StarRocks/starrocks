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
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.iceberg.view.View;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
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

        Database db = getDb(new ConnectContext(), stmt.getDbName());
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

        Database db = getDb(new ConnectContext(), stmt.getDbName());
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
        Table icebergTable = getTable(new ConnectContext(), stmt.getDbName(), stmt.getTableName());

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
            throw e;
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

        Table table = getTable(new ConnectContext(), dbName, tblName);
        return icebergCatalog.listPartitionNames((IcebergTable) table, requestContext, jobPlanningExecutor);
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        String dbName = table.getCatalogDBName();
        String tableName = table.getCatalogTableName();

        PredicateSearchKey key = PredicateSearchKey.of(dbName, tableName, params);
        triggerIcebergPlanFilesIfNeeded(key, table);

        List<FileScanTask> icebergScanTasks = splitTasks.get(key);
        if (icebergScanTasks == null) {
            throw new StarRocksConnectorException("Missing iceberg split task for table:[{}.{}]. predicate:[{}]",
                    dbName, tableName, params.getPredicate());
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

        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setPredicate(item.getPredicate())
                .setLimit(item.getLimit())
                .setTableVersionRange(versionRange)
                .build();

        key = PredicateSearchKey.of(dbName, tableName, params);
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

        triggerIcebergPlanFilesIfNeeded(key, icebergTable, tracers, connectContext);
        return true;
    }

    @Override
    public SerializedMetaSpec getSerializedMetaSpec(String dbName, String tableName, long snapshotId, String serializedPredicate,
                                                    MetadataTableType metadataTableType) {
        List<RemoteMetaSplit> remoteMetaSplits = new ArrayList<>();
        IcebergTable icebergTable = (IcebergTable) getTable(new ConnectContext(), dbName, tableName);
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

    private void triggerIcebergPlanFilesIfNeeded(PredicateSearchKey key, Table table) {
        triggerIcebergPlanFilesIfNeeded(key, table, null, ConnectContext.get());
    }

    private void triggerIcebergPlanFilesIfNeeded(PredicateSearchKey key, Table table,
                                                 Tracers tracers, ConnectContext connectContext) {
        if (!scannedTables.contains(key)) {
            tracers = tracers == null ? Tracers.get() : tracers;
            try (Timer ignored = Tracers.watchScope(tracers, EXTERNAL, "ICEBERG.processSplit." + key)) {
                collectTableStatisticsAndCacheIcebergSplit(key, table, tracers, connectContext);
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

        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setPredicate(predicate)
                .setLimit(limit)
                .setTableVersionRange(version)
                .build();

        PredicateSearchKey key = PredicateSearchKey.of(dbName, tableName, params);
        triggerIcebergPlanFilesIfNeeded(key, icebergTable);

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
                                                            Tracers tracers,
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

        List<ScalarOperator> scalarOperators = Utils.extractConjuncts(key.getPredicate());
        ScalarOperatorToIcebergExpr.IcebergContext icebergContext = new ScalarOperatorToIcebergExpr.IcebergContext(schema);
        Expression icebergPredicate = new ScalarOperatorToIcebergExpr().convert(scalarOperators, icebergContext);

        boolean enableCollectColumnStatistics = enableCollectColumnStatistics(connectContext);

        List<FileScanTask> icebergScanTasks = Lists.newArrayList();
        try (CloseableIterator<FileScanTask> iterator =
                     buildFileScanTaskIterator((IcebergTable) table, icebergPredicate, snapshotId,
                             connectContext, enableCollectColumnStatistics)) {
            while (iterator.hasNext()) {
                FileScanTask scanTask = iterator.next();

                FileScanTask icebergSplitScanTask = scanTask;
                if (enableCollectColumnStatistics) {
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
        } catch (IOException e) {
            throw new StarRocksConnectorException("Failed to iter iceberg file scan iterator", e);
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

        String dbName = table.getCatalogDBName();
        String tableName = table.getCatalogTableName();
        PredicateSearchKey predicateSearchKey = PredicateSearchKey.of(dbName, tableName, params);
        RemoteFileInfoSource baseSource;
        if (splitTasks.containsKey(predicateSearchKey)) {
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
                    dbName, tableName, params, tableFullMORParams.getMORId(), param.getMORParams());

            if (!remoteFileInfoSources.containsKey(remoteFileInfoSourceKey)) {
                IcebergRemoteSourceTrigger trigger = new IcebergRemoteSourceTrigger(baseSource, tableFullMORParams);
                // The tableFullMORParams we recorded are mainly used here. Scheduling of multiple
                // split scan nodes from one table scan node is one by one. And in the IcebergRemoteSourceTrigger,
                // multiple queues need to be filled according to different iceberg mor params when executing iceberg planing.
                // Therefore, here we initialize the remoteFileInfoSource of all iceberg mor params for the first time.
                for (IcebergMORParams morParams : tableFullMORParams.getMorParamsList()) {
                    IcebergRemoteFileInfoSourceKey key = IcebergRemoteFileInfoSourceKey.of(
                            dbName, tableName, params, tableFullMORParams.getMORId(), morParams);
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
        CloseableIterator<FileScanTask> iterator =
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

            @Override
            public void close() {
                try {
                    iterator.close();
                } catch (Exception ignore) {
                }
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

    private CloseableIterator<FileScanTask> buildFileScanTaskIterator(IcebergTable icebergTable,
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
        IcebergMetricsReporter metricsReporter = icebergTable.getIcebergMetricsReporter();
        if (metricsReporter == null) {
            throw new StarRocksConnectorException("IcebergMetricsReporter is null for table: " +
                    dbName + "." + tableName);
        }
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
        return new CloseableIterator<>() {
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
                    } finally {
                        //record scan metrics profile
                        if (tableScan instanceof StarRocksIcebergTableScan) {
                            IcebergMetricsReporter metricsReporter =
                                    ((StarRocksIcebergTableScan) tableScan).getMetricsReporter();
                            if (metricsReporter.getScanReport() != null) {
                                String name = "ICEBERG.ScanMetrics." +
                                        ((StarRocksIcebergTableScan) tableScan).getIcebergTableName().toString();
                                String value = metricsReporter.getScanReport().toString();
                                Tracers.record(Tracers.Module.EXTERNAL, name, value);
                            }
                        }
                    }
                    hasMore = false;
                }
            }

            @Override
            public void close() {
                try {
                    if (fileScanTaskIterator != null) {
                        fileScanTaskIterator.close();
                    }
                } catch (Exception ignore) {
                }

                try {
                    if (fileScanTaskIterable != null) {
                        fileScanTaskIterable.close();
                    }
                } catch (Exception ignore) {
                } finally {
                    fileScanTaskIterator = null;
                    fileScanTaskIterable = null;
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

        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setPredicate(predicate)
                .setLimit(limit)
                .setSnapshotId(snapshotId)
                .build();

        PredicateSearchKey key = PredicateSearchKey.of(
                icebergTable.getCatalogDBName(), icebergTable.getCatalogTableName(), params);

        triggerIcebergPlanFilesIfNeeded(key, icebergTable);

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
            return statisticProvider.getTableStatistics(icebergTable, columns, session, params);
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
                icebergCatalog.refreshTable(dbName, tableName, new ConnectContext(), jobPlanningExecutor);
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
        finishSink(dbName, tableName, commitInfos, branch, null);
    }

    @Override
    public void finishSink(String dbName, String tableName, List<TSinkCommitInfo> commitInfos, String branch, Object extra) {
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

        IcebergTable table = (IcebergTable) getTable(new ConnectContext(), dbName, tableName);
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
                        IcebergUtil.tableDataLocation(nativeTbl), dataFile.partition_path);
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

    public static String getIcebergRelativePartitionPath(String tableDataLocation, String partitionLocation) {
        tableDataLocation = tableDataLocation.endsWith("/") ? tableDataLocation.substring(0, tableDataLocation.length() - 1) :
                tableDataLocation;
        String path = PartitionUtil.getSuffixName(tableDataLocation, partitionLocation);
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