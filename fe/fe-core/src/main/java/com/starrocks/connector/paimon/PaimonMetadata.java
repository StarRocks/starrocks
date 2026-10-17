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
import com.starrocks.catalog.PaimonView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.tvr.TvrDeltaStats;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorTableVersion;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.index.ConnectorIndexDescriptor;
import com.starrocks.connector.index.ConnectorIndexDescriptor.IndexType;
import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.ConnectorIndexMetadata.TableType;
import com.starrocks.connector.index.ConnectorIndexResult;
import com.starrocks.connector.index.ConnectorIndexShard;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.connector.statistics.ConnectorNdvEstimator;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.KeyPartitionRef;
import com.starrocks.sql.ast.TruncateTablePartitionStmt;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.system.SnapshotsTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.utils.TagManager;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewImpl;

import java.io.FileNotFoundException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.connector.paimon.PaimonIndexMetadataReader.toConnectorIndexTableType;
import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;
import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;

public class PaimonMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(PaimonMetadata.class);

    public static final List<String> PARTITION_NULL_VALUES = ImmutableList.of("__DEFAULT_PARTITION__", "null");
    private static final String VIEW_DIALECTS_KEY = "starrocks";
    private final Catalog paimonNativeCatalog;
    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private final Map<Identifier, Table> tables = new ConcurrentHashMap<>();
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<PredicateSearchKey, PaimonSplitsInfo> paimonSplits = new ConcurrentHashMap<>();
    private final ConnectorProperties properties;
    private final PaimonIndexMetadataCache indexMetadataCache;
    private final Map<Identifier, Map<String, Partition>> partitionInfos = new ConcurrentHashMap<>();
    private final ThreadLocal<Map<String, String>> branches = new ThreadLocal<>();

    public PaimonMetadata(String catalogName, HdfsEnvironment hdfsEnvironment, Catalog paimonNativeCatalog,
                          ConnectorProperties properties) {
        this(catalogName, hdfsEnvironment, paimonNativeCatalog, properties,
                new PaimonIndexMetadataCache(Duration.ofMinutes(10), 1000L));
    }

    PaimonMetadata(String catalogName, HdfsEnvironment hdfsEnvironment, Catalog paimonNativeCatalog,
                   ConnectorProperties properties, PaimonIndexMetadataCache indexMetadataCache) {
        this.paimonNativeCatalog = paimonNativeCatalog;
        this.hdfsEnvironment = hdfsEnvironment;
        this.catalogName = catalogName;
        this.properties = properties;
        this.indexMetadataCache = indexMetadataCache;
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
            List<String> tableIdentifiers = paimonNativeCatalog.listTables(dbName);
            List<String> viewIdentifiers = paimonNativeCatalog.listViews(dbName);
            if (!viewIdentifiers.isEmpty()) {
                tableIdentifiers.addAll(viewIdentifiers);
            }
            return tableIdentifiers;
        } catch (Catalog.DatabaseNotExistException e) {
            throw new StarRocksConnectorException("Database %s not exists", dbName);
        }
    }

    @Override
    public void createView(ConnectContext context, CreateViewStmt stmt) {
        String dbName = stmt.getDbName();
        String viewName = stmt.getTable();
        String viewDefinition = ConnectorViewDefinition.fromCreateViewStmt(stmt).getInlineViewDef();
        RowType rowType = ColumnTypeConverter.toPaimonRowType(stmt.getColumns());
        Identifier identifier = new org.apache.paimon.catalog.Identifier(dbName, viewName);
        Map<String, String> dialects = new HashMap<>(1);
        dialects.put(VIEW_DIALECTS_KEY, viewDefinition);
        View view = new ViewImpl(identifier, rowType.getFields(), viewDefinition, dialects, stmt.getComment(), new HashMap<>());
        try {
            paimonNativeCatalog.createView(new Identifier(dbName, viewName), view, stmt.isSetIfNotExists());
        } catch (Catalog.ViewAlreadyExistException | Catalog.DatabaseNotExistException e) {
            throw new StarRocksConnectorException(
                    String.format("Paimon createView error for %s.%s", dbName, viewName), e);
        }
    }

    @Override
    public void dropTable(ConnectContext context, DropTableStmt stmt) {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        Table paimonTable = getTable(context, stmt.getDbName(), stmt.getTableName());
        if (paimonTable == null) {
            return;
        }
        try {
            if (paimonTable.isPaimonView()) {
                paimonNativeCatalog.dropView(new Identifier(dbName, tableName), stmt.isForceDrop());
                indexMetadataCache.invalidateTable(catalogName, dbName, tableName);
                return;
            }
            paimonNativeCatalog.dropTable(new Identifier(dbName, tableName), stmt.isForceDrop());
            indexMetadataCache.invalidateTable(catalogName, dbName, tableName);
        } catch (Exception e) {
            throw new StarRocksConnectorException(
                    String.format("Paimon dropTable error for %s.%s", dbName, tableName), e);
        }
    }

    private void updateAllPartitionInfos(String databaseName, String tableName) {
        Identifier identifier = new Identifier(databaseName, tableName);
        org.apache.paimon.table.Table paimonTable;
        RowType dataTableRowType;
        if (!this.partitionInfos.containsKey(identifier)) {
            this.partitionInfos.put(identifier, new ConcurrentHashMap<>());
        }
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
            Set<String> currentPartitionNames = new HashSet<>();
            boolean partitionLegacyName = getPartitionLegacyName(paimonTable);
            for (org.apache.paimon.partition.Partition partition : partitions) {
                String partitionPath = PartitionPathUtils.generatePartitionPath(partition.spec(), dataTableRowType,
                        /* onlyValue */ false);
                String[] partitionValues =
                        Arrays.stream(partitionPath.split("/")).map(part -> part.split("=")[1]).toArray(String[]::new);
                Partition srPartition =
                        getPartition(partition, partitionColumnNames, partitionColumnTypes, partitionValues, partitionLegacyName);
                currentPartitionNames.add(srPartition.getPartitionName());
                this.partitionInfos.get(identifier).put(srPartition.getPartitionName(), srPartition);
            }
            this.partitionInfos.get(identifier).keySet().retainAll(currentPartitionNames);
        } catch (Catalog.TableNotExistException e) {
            LOG.error("Failed to update partition info of paimon table {}.{}.", databaseName, tableName, e);
        }
    }

    private boolean getPartitionLegacyName(org.apache.paimon.table.Table paimonTable) {
        String partitionLegacyName = paimonTable.options().get(CoreOptions.PARTITION_GENERATE_LEGACY_NAME.key());
        //If the user does not explicitly set this option, the result is null,but its default value is true.
        return StringUtils.isEmpty(partitionLegacyName) || Boolean.parseBoolean(partitionLegacyName);
    }

    private Partition getPartition(org.apache.paimon.partition.Partition partition,
                                   List<String> partitionColumnNames,
                                   List<DataType> partitionColumnTypes,
                                   String[] partitionValues,
                                   boolean partitionLegacyName) {
        if (partitionValues.length != partitionColumnNames.size()) {
            String errorMsg = String.format("The length of partitionValues %s is not equal to " +
                    "the partitionColumnNames %s.", partitionValues.length, partitionColumnNames.size());
            throw new IllegalArgumentException(errorMsg);
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partitionValues.length; i++) {
            String column = partitionColumnNames.get(i);
            String value = partitionValues[i].trim();
            if (partitionColumnTypes.get(i) instanceof DateType && partitionLegacyName
                    && !PARTITION_NULL_VALUES.contains(value)) {
                value = DateTimeUtils.formatDate(Integer.parseInt(value));
            }
            sb.append(column).append("=").append(value);
            sb.append("/");
        }
        sb.deleteCharAt(sb.length() - 1);
        String partitionName = sb.toString();
        Timestamp lastUpdateTime = Timestamp.fromEpochMillis(partition.lastFileCreationTime());
        return new Partition(partitionName, convertToSystemDefaultTime(lastUpdateTime),
                partition.recordCount(), partition.fileSizeInBytes(), partition.fileCount());
    }

    private Long convertToSystemDefaultTime(Timestamp lastUpdateTime) {
        LocalDateTime localDateTime = lastUpdateTime.toLocalDateTime();
        ZoneId zoneId = ZoneId.systemDefault();
        ZonedDateTime zonedDateTime = localDateTime.atZone(zoneId);
        return zonedDateTime.toInstant().toEpochMilli();
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName,
                                           ConnectorMetadataRequestContext requestContext) {
        Identifier identifier = new Identifier(databaseName, tableName);
        updateAllPartitionInfos(databaseName, tableName);
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
            Database db = new Database(CONNECTOR_ID_GENERATOR.getNextId().asLong(), dbName);
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
            return getView(dbName, tblName);
        }
        List<DataField> fields = paimonNativeTable.rowType().getFields();
        List<Column> fullSchema = ColumnTypeConverter.fromPaimonSchemas(fields);
        String comment = "";
        if (paimonNativeTable.comment().isPresent()) {
            comment = paimonNativeTable.comment().get();
        }
        PaimonTable table = new PaimonTable(this.catalogName, dbName, tblName, fullSchema, paimonNativeTable);
        table.setComment(comment);
        tables.put(identifier, table);
        return table;
    }

    public Table getView(String dbName, String viewName) {
        Identifier identifier = new Identifier(dbName, viewName);
        if (tables.containsKey(identifier)) {
            return tables.get(identifier);
        }
        org.apache.paimon.view.View paimonNativeView;
        try {
            paimonNativeView = paimonNativeCatalog.getView(new Identifier(dbName, viewName));
        } catch (Catalog.ViewNotExistException e) {
            LOG.error("Paimon view {}.{} does not exist.", dbName, viewName, e);
            return null;
        }
        PaimonView view = getPaimonView(this.catalogName, dbName, viewName, paimonNativeView);
        tables.put(identifier, view);
        return view;
    }

    private PaimonView getPaimonView(String catalogName, String dbName, String viewName, View paimonNativeView) {
        List<DataField> fields = paimonNativeView.rowType().getFields();
        List<Column> fullSchema = ColumnTypeConverter.fromPaimonSchemas(fields);
        String comment = "";
        Optional<String> commentOptional = paimonNativeView.comment();
        if (commentOptional.isPresent()) {
            comment = commentOptional.get();
        }
        String query;
        if (paimonNativeView.dialects().containsKey(VIEW_DIALECTS_KEY)) {
            query = paimonNativeView.dialects().get(VIEW_DIALECTS_KEY);
        } else {
            query = paimonNativeView.query();
        }
        PaimonView view = new PaimonView(CONNECTOR_ID_GENERATOR.getNextId().asLong(),
                catalogName, dbName, viewName, fullSchema, query);
        view.setComment(comment);
        return view;
    }

    @Override
    public TvrVersionRange getTableVersionRange(String dbName, Table table,
                                                Optional<ConnectorTableVersion> startVersion,
                                                Optional<ConnectorTableVersion> endVersion) {
        PaimonTable paimonTable = (PaimonTable) table;
        Optional<Long> start = startVersion.map(v -> getSnapshotIdFromVersion(paimonTable.getNativeTable(), v));
        Optional<Long> end = endVersion.map(v -> getSnapshotIdFromVersion(paimonTable.getNativeTable(), v));
        if (start.isEmpty() && end.isEmpty()) {
            long snapshotId = -1L;
            try {
                Optional<Snapshot> latestSnapshot = paimonTable.getNativeTable().latestSnapshot();
                if (latestSnapshot.isPresent()) {
                    snapshotId = latestSnapshot.get().id();
                }
            } catch (Exception e) {
                // System table does not have snapshotId, ignore it.
                LOG.warn("Cannot get snapshot because {}", e.getMessage());
            }
            return TvrTableSnapshot.of(Optional.of(snapshotId));
        } else {
            return TvrTableDelta.of(start, end);
        }
    }

    public long getSnapshotIdFromVersion(org.apache.paimon.table.Table table, ConnectorTableVersion version) {
        switch (version.getPointerType()) {
            case TEMPORAL: //specify timestamp
                return getSnapshotIdFromTemporalVersion(table, version.getConstantOperator());
            case VERSION: //specify snapshotId、branch、tag
                return getTargetSnapshotIdFromVersion(table, version.getConstantOperator());
            case UNKNOWN:
            default:
                throw new StarRocksConnectorException("Unknown version type %s", version.getPointerType());
        }
    }

    private long getSnapshotIdFromTemporalVersion(org.apache.paimon.table.Table table, ConstantOperator version) {
        long snapshotId = -1L;
        try {
            if (version.getType() != com.starrocks.type.DateType.DATETIME &&
                    version.getType() != com.starrocks.type.DateType.DATE &&
                    version.getType() != com.starrocks.type.VarcharType.VARCHAR) {
                throw new StarRocksConnectorException("Unsupported type for table temporal version: %s." +
                        " You should use timestamp type", version);
            }
            Optional<ConstantOperator> timestampVersion = version.castTo(com.starrocks.type.DateType.DATETIME);
            if (timestampVersion.isEmpty()) {
                throw new StarRocksConnectorException("Unsupported type for table temporal version: %s." +
                        " You should use timestamp type", version);
            }
            LocalDateTime time = timestampVersion.get().getDatetime();
            long epochMillis = Duration.ofSeconds(time.atZone(TimeUtils.getTimeZone().toZoneId()).toEpochSecond()).toMillis();
            SnapshotManager snapshotManager = ((DataTable) table).snapshotManager();
            Snapshot snapshot = snapshotManager.earlierOrEqualTimeMills(epochMillis);
            if (snapshot != null) {
                snapshotId = snapshot.id();
            } else {
                throw new StarRocksConnectorException("No version history table %s at or before %s",
                        table.fullName(), time);
            }
        } catch (Exception e) {
            throw new StarRocksConnectorException("Invalid temporal version [%s]", version, e);
        }
        return snapshotId;
    }

    private long getTargetSnapshotIdFromVersion(org.apache.paimon.table.Table table, ConstantOperator version) {
        long snapshotId = -1L;
        //specify snapshotId
        if (version.getType() == com.starrocks.type.IntegerType.TINYINT ||
                version.getType() == com.starrocks.type.IntegerType.SMALLINT ||
                version.getType() == com.starrocks.type.IntegerType.INT ||
                version.getType() == com.starrocks.type.IntegerType.BIGINT) {
            snapshotId = version.castTo(com.starrocks.type.IntegerType.BIGINT).get().getBigint();
            if (!((DataTable) table).snapshotManager().snapshotExists(snapshotId)) {
                throw new StarRocksConnectorException("%s does not include snapshot: %s",
                        table.fullName(), snapshotId);
            }
            //specify branch、tag
        } else if (version.getType() == com.starrocks.type.VarcharType.VARCHAR) {
            String refName = version.getVarchar().toLowerCase();
            String[] refNameParts = refName.split(":");
            org.apache.paimon.table.Table paimonTable;
            if (refNameParts.length == 2 &&
                    (refNameParts[0].equals("branch") || refNameParts[0].equals("tag"))) {
                if (refNameParts[0].equals("branch")) {
                    //if branch, format like branch:b_1, return the latest snapshot of the branch
                    String[] tableFullName = table.fullName().split("\\.");
                    Identifier identifier = new Identifier(tableFullName[0], tableFullName[1], refNameParts[1]);
                    try {
                        paimonTable = paimonNativeCatalog.getTable(identifier);
                    } catch (Catalog.TableNotExistException e) {
                        throw new StarRocksConnectorException("%s does not include branch: %s",
                                table.fullName(), refNameParts[1]);
                    }
                    setRequestedBranch(table, identifier.getBranchNameOrDefault());
                    snapshotId = paimonTable.latestSnapshot().isPresent() ? paimonTable.latestSnapshot().get().id() : -1L;
                } else {
                    //if tag, format like tag:t_1, return the snapshot that the tag referenced
                    TagManager tagManager =  ((DataTable) table).tagManager();
                    if (!tagManager.tagExists(refNameParts[1])) {
                        throw new StarRocksConnectorException("%s does not include tag: %s",
                                table.fullName(), refNameParts[1]);
                    }
                    snapshotId = tagManager.tagObjects().stream()
                            .filter(p -> p.getValue().equals(refNameParts[1]))
                            .map(p -> p.getKey().id())
                            .findFirst()
                            .get();
                }
            } else {
                throw new StarRocksConnectorException("Please input correct format like branch:branch_name or tag:tag_name");
            }
        } else {
            throw new StarRocksConnectorException("Unsupported type for table version: " + version);
        }

        return snapshotId;
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
    public TvrTableSnapshot getCurrentTvrSnapshot(String dbName, Table table) {
        PaimonTable paimonTable = (PaimonTable) table;
        Optional<Long> snapshotId = paimonTable.getNativeTable().latestSnapshot().map(snapshot -> snapshot.id());
        return TvrTableSnapshot.of(snapshotId);
    }

    @Override
    public List<TvrTableDeltaTrait> listTableDeltaTraits(String dbName, Table table,
                                                         TvrTableSnapshot fromSnapshotExclusive,
                                                         TvrTableSnapshot toSnapshotInclusive) {
        if (fromSnapshotExclusive.equals(toSnapshotInclusive)) {
            return Collections.emptyList();
        }
        // fromSnapshotExclusive can be empty, but toSnapshotInclusive must have a valid snapshot ID
        final long fromSnapshotIdExclusive = fromSnapshotExclusive.getSnapshotId();
        final long toSnapshotIdInclusive =
                toSnapshotInclusive.end().orElseThrow(() -> new StarRocksConnectorException(
                        "toSnapshotInclusive must have a valid snapshot ID"));
        final PaimonTable paimonTable = (PaimonTable) table;
        final org.apache.paimon.table.Table paimonNativeTable = paimonTable.getNativeTable();
        if (paimonNativeTable instanceof org.apache.paimon.table.DataTable) {
            org.apache.paimon.table.DataTable paimonDataTable = (org.apache.paimon.table.DataTable) paimonNativeTable;
            SnapshotManager snapshotManager = new SnapshotManager(paimonNativeTable.fileIO(),
                    paimonDataTable.location(), null, null, null);
            // the result is ensured to sort by snapshot id
            Iterator<Snapshot> iterator = snapshotManager.snapshotsWithinRange(
                    Optional.of(toSnapshotIdInclusive),
                    Optional.of(fromSnapshotIdExclusive)
            );
            List<TvrTableDeltaTrait> result = new ArrayList<>();
            Snapshot lastSnapshot = null;
            while (iterator.hasNext()) {
                Snapshot currentSnapshot = iterator.next();
                if (lastSnapshot != null) {
                    long lastRecordCount = lastSnapshot.totalRecordCount();
                    long currentRecordCount = currentSnapshot.totalRecordCount();
                    long deltaRecordCount = currentRecordCount - lastRecordCount;
                    TvrTableDelta delta = TvrTableDelta.of(lastSnapshot.id(), currentSnapshot.id());
                    TvrDeltaStats stats = TvrDeltaStats.of(deltaRecordCount, 0L);
                    if (currentSnapshot.commitKind() == Snapshot.CommitKind.APPEND) {
                        result.add(TvrTableDeltaTrait.ofMonotonic(delta, stats));
                    } else {
                        result.add(TvrTableDeltaTrait.ofRetractable(delta, stats));
                    }
                } else {
                    // if start snapshot is min, add it into result
                    if (fromSnapshotExclusive.isEmpty()) {
                        long currentRecordCount = currentSnapshot.totalRecordCount();
                        TvrTableDelta delta = TvrTableDelta.of(fromSnapshotIdExclusive, currentSnapshot.id());
                        TvrDeltaStats stats = TvrDeltaStats.of(currentRecordCount, 0L);
                        if (currentSnapshot.commitKind() == Snapshot.CommitKind.APPEND) {
                            result.add(TvrTableDeltaTrait.ofMonotonic(delta, stats));
                        } else {
                            result.add(TvrTableDeltaTrait.ofRetractable(delta, stats));
                        }
                    } else {
                        // ensure this snapshot id is equal to from
                        if (currentSnapshot.id() != fromSnapshotIdExclusive) {
                            throw new SemanticException(String.format("Expect from snapshot id:%s, but got:%s",
                                    fromSnapshotIdExclusive, currentSnapshot.id()));
                        }
                    }
                }
                lastSnapshot = currentSnapshot;
            }
            return result;
        } else {
            throw new SemanticException("Incremental read unsupported table type: " + paimonNativeTable.getClass());
        }
    }

    /**
     * If tvrVersionRange is present, build an incremental scan from start snapshot to end snapshot.
     * @param nativeTable paimon native table
     * @param tvrVersionRange input tvrVersionRange
     * @return a native paimon table with
     */
    private org.apache.paimon.table.Table getNativeTable(org.apache.paimon.table.Table nativeTable,
                                                         TvrVersionRange tvrVersionRange) {
        Map<String, String> dynamicOptions = new HashMap<>();
        if (tvrVersionRange != null && tvrVersionRange.start().isPresent()
                && tvrVersionRange.end().isPresent()) {
            // Build dynamic options for incremental-between
            TvrTableDelta tableDelta = (TvrTableDelta) tvrVersionRange;
            long startSnapshotId = tableDelta.start().get();
            long endSnapshotId = tableDelta.end().get();
            dynamicOptions.put("incremental-between", startSnapshotId + "," + endSnapshotId);
            dynamicOptions.put("incremental-between-scan-mode", "auto");
        }
        return nativeTable.copy(dynamicOptions);
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        PaimonTable paimonTable = (PaimonTable) table;
        long snapshotId = -1L;
        String currentBranch = consumeRequestedBranch(paimonTable);
        Identifier identifier = new Identifier(paimonTable.getCatalogDBName(),
                paimonTable.getCatalogTableName(), currentBranch);
        if (!new Identifier(paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName()).isSystemTable()) {
            try {
                paimonTable.setPaimonNativeTable(paimonNativeCatalog.getTable(identifier));
            } catch (Catalog.TableNotExistException e) {
                throw new StarRocksConnectorException("%s does not include branch: %s",
                        paimonTable.getCatalogTableName(), currentBranch);
            }
        }
        TvrVersionRange tvrVersionRange = params.getTableVersionRange();
        snapshotId = tvrVersionRange.end().isPresent() ? tvrVersionRange.end().get() : -1L;

        org.apache.paimon.table.Table versionedPaimonTable = getNativeTable(paimonTable.getNativeTable(),
                tvrVersionRange);
        Map<String, String> options = new HashMap<>();
        if (params.isDisableGlobalIndex()) {
            options.put("global-index.enabled", "false");
        }
        if (snapshotId >= 0) {
            options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshotId));
        }
        if (versionedPaimonTable instanceof FileStoreTable
                && !versionedPaimonTable.options().containsKey(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key())
                && !versionedPaimonTable.options().containsKey(CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key())) {
            // Paimon's FAST default only searches indexed row IDs. FULL also scans row IDs not
            // covered by an index, preserving SQL correctness while an index is being refreshed.
            options.put(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(),
                    CoreOptions.GlobalIndexSearchMode.FULL.toString());
        }

        org.apache.paimon.table.Table paimonNativeTable = versionedPaimonTable.copy(options);

        GetRemoteFilesParams copyParams = params.copy();
        copyParams.setTableVersionRange(TvrTableSnapshot.of(snapshotId));

        PredicateSearchKey filter = PredicateSearchKey.of(paimonTable.getCatalogDBName(),
                paimonTable.getCatalogTableName(), copyParams);

        boolean cacheSplits = params.getConnectorIndexResult() == null && !params.isDisableGlobalIndex();
        PaimonSplitsInfo cachedSplits = cacheSplits ? paimonSplits.get(filter) : null;
        if (cachedSplits == null) {
            ReadBuilder readBuilder = paimonNativeTable.newReadBuilder();
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
            if (!params.isDisableGlobalIndex()) {
                resolvePaimonGlobalIndexResult(params.getConnectorIndexResult(),
                        paimonNativeTable, currentBranch, tvrVersionRange).ifPresent(scan::withGlobalIndexResult);
            }
            PaimonMetricRegistry paimonMetricRegistry = new PaimonMetricRegistry();
            List<Split> splits = scan.withMetricRegistry(paimonMetricRegistry).plan().splits();
            traceScanMetrics(paimonMetricRegistry, splits, table.getCatalogTableName(), predicates);

            PaimonSplitsInfo paimonSplitsInfo = new PaimonSplitsInfo(predicates, splits);
            if (cacheSplits) {
                paimonSplits.put(filter, paimonSplitsInfo);
            }
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

    @Override
    public Optional<ConnectorIndexResult> evaluateIndex(
            Table table, IndexCondition condition, TvrVersionRange versionRange) {
        if (!(table instanceof PaimonTable) || condition == null
                || condition.getFieldIds().isEmpty() || !isIndexSnapshot(versionRange)) {
            return Optional.empty();
        }
        PaimonTable paimonTable = (PaimonTable) table;
        org.apache.paimon.table.Table nativeTable = paimonTable.getNativeTable();
        if (!(nativeTable instanceof FileStoreTable)
                || !DEFAULT_MAIN_BRANCH.equals(resolveIndexBranch(nativeTable.options(), getRequestedBranch(paimonTable)))
                || !Boolean.parseBoolean(nativeTable.options().getOrDefault("global-index.enabled", "true"))) {
            return Optional.empty();
        }
        FileStoreTable fileStoreTable = (FileStoreTable) nativeTable;
        if (toConnectorIndexTableType(fileStoreTable.schema()) != TableType.DATA_EVOLUTION) {
            return Optional.empty();
        }
        PaimonGlobalIndexRequest request = PaimonGlobalIndexRequest.create(
                fileStoreTable.location().toString(), DEFAULT_MAIN_BRANCH, versionRange.end().orElseThrow(),
                fileStoreTable.schema().id(), fileStoreTable.schema().timeMillis(), condition);
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "PaimonGlobalIndex.Evaluate")) {
            return Optional.of(new PaimonGlobalIndexService(paimonTable, request).evaluate());
        }
    }

    private static boolean isIndexSnapshot(TvrVersionRange versionRange) {
        return versionRange != null && versionRange.start().isEmpty()
                && versionRange.end().isPresent() && versionRange.end().get() >= 0;
    }

    static Optional<GlobalIndexResult> resolvePaimonGlobalIndexResult(
            ConnectorIndexResult result, org.apache.paimon.table.Table nativeTable,
            String branch, TvrVersionRange versionRange) {
        if (!(result instanceof PaimonGlobalIndexResult) || !(nativeTable instanceof FileStoreTable)
                || !isIndexSnapshot(versionRange)) {
            return Optional.empty();
        }
        FileStoreTable table = (FileStoreTable) nativeTable;
        PaimonGlobalIndexResult paimonResult = (PaimonGlobalIndexResult) result;
        if (toConnectorIndexTableType(table.schema()) != TableType.DATA_EVOLUTION
                || !table.location().toString().equals(paimonResult.getTablePath())
                || !branch.equals(paimonResult.getBranch())
                || paimonResult.getSnapshotId() != versionRange.end().orElseThrow()) {
            Tracers.record(EXTERNAL, "PaimonGlobalIndex.Result", "rejected: incompatible table, branch or snapshot");
            return Optional.empty();
        }
        return Optional.of(paimonResult.getResult());
    }

    @Override
    public List<ConnectorIndexShard> getIndexShards(
            Table table, long snapshotId, Map<Integer, IndexType> requiredIndexes) {
        if (!(table instanceof PaimonTable) || snapshotId < 0 || requiredIndexes.isEmpty()
                || requiredIndexes.values().stream().anyMatch(type -> type != IndexType.RANGE)) {
            return List.of();
        }
        PaimonTable paimonTable = (PaimonTable) table;
        org.apache.paimon.table.Table nativeTable = paimonTable.getNativeTable();
        if (!(nativeTable instanceof FileStoreTable)
                || !DEFAULT_MAIN_BRANCH.equals(resolveIndexBranch(nativeTable.options(), getRequestedBranch(paimonTable)))) {
            return List.of();
        }
        FileStoreTable fileStoreTable = (FileStoreTable) nativeTable;
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "PaimonGlobalIndex.ValidateCoverage")) {
            Optional<Snapshot> resolved = resolveIndexSnapshot(fileStoreTable, TvrTableSnapshot.of(snapshotId));
            if (resolved.isEmpty()) {
                return List.of();
            }
            Snapshot snapshot = resolved.get();
            TableSchema schema = fileStoreTable.schemaManager().schema(snapshot.schemaId());
            if (schema == null || toConnectorIndexTableType(schema) != TableType.DATA_EVOLUTION
                    || toConnectorIndexTableType(fileStoreTable.schema()) != TableType.DATA_EVOLUTION
                    || !schema.idToFieldMap().keySet().containsAll(requiredIndexes.keySet())
                    || !indexMetadataCache.getStableScalarFields(catalogName, paimonTable.getCatalogDBName(),
                            paimonTable.getCatalogTableName(), fileStoreTable).containsAll(requiredIndexes.keySet())) {
                return List.of();
            }

            List<Range> dataRanges = new ArrayList<>();
            for (Split split : fileStoreTable.newSnapshotReader().withSnapshot(snapshot)
                    .withMode(ScanMode.ALL).read().splits()) {
                if (!(split instanceof DataSplit)) {
                    return List.of();
                }
                for (DataFileMeta file : ((DataSplit) split).dataFiles()) {
                    if (file.firstRowId() == null) {
                        return List.of();
                    }
                    dataRanges.add(file.nonNullRowIdRange());
                }
            }
            List<Range> dataCoverage = mergeRanges(dataRanges);
            if (dataCoverage.isEmpty()) {
                return List.of();
            }
            Map<Integer, List<Range>> indexRanges = new HashMap<>();
            for (IndexManifestEntry entry : fileStoreTable.store().newIndexFileHandler()
                    .scan(snapshot, unused -> true)) {
                GlobalIndexMeta globalIndex = entry.indexFile().globalIndexMeta();
                if (globalIndex == null || !requiredIndexes.containsKey(globalIndex.indexFieldId())) {
                    continue;
                }
                // The native field-ID API wraps files in one "union" reader per provider.
                // Restrict this first scalar executor to unambiguous BTree-only fields.
                if (!"btree".equals(entry.indexFile().indexType())) {
                    Tracers.record(EXTERNAL, "PaimonGlobalIndex.Coverage", "mixed providers: use SDK planning");
                    return List.of();
                }
                indexRanges.computeIfAbsent(globalIndex.indexFieldId(), unused -> new ArrayList<>())
                        .add(globalIndex.rowRange());
            }
            List<Range> indexShards = null;
            for (Integer fieldId : requiredIndexes.keySet()) {
                List<Range> physicalRanges = mergeRanges(indexRanges.getOrDefault(fieldId, List.of()), false);
                if (!covers(mergeRanges(physicalRanges), dataCoverage)) {
                    Tracers.record(EXTERNAL, "PaimonGlobalIndex.Coverage", "partial: fall back to SDK planning");
                    return List.of();
                }
                indexShards = indexShards == null ? physicalRanges
                        : intersectRangesPreservingBoundaries(indexShards, physicalRanges);
                if (indexShards.isEmpty()) {
                    return List.of();
                }
            }
            return indexShards.stream().map(range -> new ConnectorIndexShard(range.from, range.to))
                    .collect(Collectors.toList());
        } catch (RuntimeException e) {
            if (indexMetadataCache.shouldLogFailure(System.currentTimeMillis())) {
                LOG.warn("Failed to validate Paimon Global Index coverage at snapshot {}", snapshotId, e);
            }
            return List.of();
        }
    }

    static List<Range> mergeRanges(List<Range> ranges) {
        return mergeRanges(ranges, true);
    }

    static List<Range> mergeRanges(List<Range> ranges, boolean mergeAdjacent) {
        if (ranges.isEmpty()) {
            return Collections.emptyList();
        }
        List<Range> sorted = ranges.stream()
                .sorted(Comparator.comparingLong((Range range) -> range.from).thenComparingLong(range -> range.to))
                .collect(Collectors.toList());
        List<Range> merged = new ArrayList<>();
        long from = sorted.get(0).from;
        long to = sorted.get(0).to;
        for (int i = 1; i < sorted.size(); i++) {
            Range current = sorted.get(i);
            boolean adjacent = mergeAdjacent && to != Long.MAX_VALUE && current.from == to + 1;
            if (current.from <= to || adjacent) {
                to = Math.max(to, current.to);
            } else {
                merged.add(new Range(from, to));
                from = current.from;
                to = current.to;
            }
        }
        merged.add(new Range(from, to));
        return merged;
    }

    static List<Range> intersectRangesPreservingBoundaries(List<Range> left, List<Range> right) {
        List<Range> intersections = new ArrayList<>();
        int leftIndex = 0;
        int rightIndex = 0;
        while (leftIndex < left.size() && rightIndex < right.size()) {
            Range leftRange = left.get(leftIndex);
            Range rightRange = right.get(rightIndex);
            long from = Math.max(leftRange.from, rightRange.from);
            long to = Math.min(leftRange.to, rightRange.to);
            if (from <= to) {
                intersections.add(new Range(from, to));
            }
            if (leftRange.to <= rightRange.to) {
                leftIndex++;
            }
            if (rightRange.to <= leftRange.to) {
                rightIndex++;
            }
        }
        return intersections;
    }

    static boolean covers(List<Range> coverage, List<Range> required) {
        int coverageIndex = 0;
        for (Range target : required) {
            while (coverageIndex < coverage.size() && coverage.get(coverageIndex).to < target.from) {
                coverageIndex++;
            }
            if (coverageIndex >= coverage.size()) {
                return false;
            }
            Range candidate = coverage.get(coverageIndex);
            if (candidate.from > target.from || candidate.to < target.to) {
                return false;
            }
        }
        return true;
    }

    private void setRequestedBranch(org.apache.paimon.table.Table table, String requestedBranch) {
        Map<String, String> requestedBranches = branches.get();
        if (requestedBranches == null) {
            requestedBranches = new HashMap<>();
            branches.set(requestedBranches);
        }
        requestedBranches.put(tableBranchKey(table), requestedBranch);
    }

    private String consumeRequestedBranch(PaimonTable table) {
        Map<String, String> requestedBranches = branches.get();
        if (requestedBranches == null) {
            return DEFAULT_MAIN_BRANCH;
        }
        String requestedBranch = requestedBranches.remove(tableBranchKey(table));
        if (requestedBranches.isEmpty()) {
            branches.remove();
        }
        return requestedBranch == null ? DEFAULT_MAIN_BRANCH : requestedBranch;
    }

    private String getRequestedBranch(PaimonTable table) {
        Map<String, String> requestedBranches = branches.get();
        return requestedBranches == null
                ? DEFAULT_MAIN_BRANCH
                : requestedBranches.getOrDefault(tableBranchKey(table), DEFAULT_MAIN_BRANCH);
    }

    @Override
    public ConnectorIndexMetadata getIndexMetadata(Table table, TvrVersionRange versionRange) {
        if (!(table instanceof PaimonTable)) {
            return ConnectorIndexMetadata.empty();
        }
        PaimonTable paimonTable = (PaimonTable) table;
        if (versionRange != null && versionRange.end().isPresent() && versionRange.end().get() < 0) {
            return ConnectorIndexMetadata.empty();
        }
        org.apache.paimon.table.Table nativeTable = paimonTable.getNativeTable();
        String currentBranch = getRequestedBranch(paimonTable);
        if (!DEFAULT_MAIN_BRANCH.equals(currentBranch)) {
            try {
                nativeTable = paimonNativeCatalog.getTable(new Identifier(
                        paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName(), currentBranch));
            } catch (Catalog.TableNotExistException e) {
                LOG.warn("Cannot resolve Paimon branch {} for index metadata on {}.{}",
                        currentBranch, paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName());
                return ConnectorIndexMetadata.empty();
            }
        }
        if (!(nativeTable instanceof FileStoreTable)) {
            return ConnectorIndexMetadata.empty();
        }

        FileStoreTable fileStoreTable = (FileStoreTable) nativeTable;
        currentBranch = resolveIndexBranch(fileStoreTable.options(), currentBranch);
        // A row-id result cannot describe an incremental-between scan. A delta whose start is
        // unbounded is still a snapshot read and is therefore safe to discover against its end.
        if (versionRange instanceof TvrTableDelta
                && versionRange.start().isPresent() && versionRange.end().isPresent()) {
            return ConnectorIndexMetadata.empty();
        }

        Optional<Snapshot> snapshot = resolveIndexSnapshot(fileStoreTable, versionRange);
        if (snapshot.isEmpty()) {
            return ConnectorIndexMetadata.empty();
        }

        Snapshot resolvedSnapshot = snapshot.get();
        long snapshotId = resolvedSnapshot.id();
        PaimonIndexMetadataCacheKey cacheKey = new PaimonIndexMetadataCacheKey(
                catalogName, paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName(),
                String.valueOf(fileStoreTable.location()), currentBranch, snapshotId,
                resolvedSnapshot.timeMillis(), resolvedSnapshot.indexManifest());
        try {
            ConnectorIndexMetadata metadata = indexMetadataCache.get(cacheKey,
                    () -> PaimonIndexMetadataReader.load(fileStoreTable, resolvedSnapshot));
            Set<Integer> stableBtreeFields = metadata.getDescriptors().stream()
                    .anyMatch(descriptor -> "btree".equals(descriptor.getProvider()))
                    ? indexMetadataCache.getStableScalarFields(catalogName, paimonTable.getCatalogDBName(),
                            paimonTable.getCatalogTableName(), fileStoreTable) : Set.of();
            return bindCurrentSchema(metadata, fileStoreTable, stableBtreeFields);
        } catch (RuntimeException e) {
            // Caffeine does not cache loader exceptions, so a transient catalog/object-store
            // failure can be retried by the next query instead of becoming an empty cached result.
            if (indexMetadataCache.shouldLogFailure(System.currentTimeMillis())) {
                LOG.warn("Failed to load Paimon index metadata for {}.{}.{} at snapshot {}",
                        catalogName, paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName(), snapshotId, e);
            }
            return ConnectorIndexMetadata.empty();
        }
    }

    static String resolveIndexBranch(Map<String, String> nativeOptions, String requestedBranch) {
        if (nativeOptions == null) {
            return requestedBranch;
        }
        String optionBranch = CoreOptions.branch(nativeOptions);
        // getRemoteFiles consumes the ThreadLocal after replacing the PaimonTable's native table.
        // In that production order the branch embedded in the actual native table is authoritative.
        // Before getRemoteFiles, keep an explicit non-main request even if a catalog implementation
        // does not expose the branch option on the copied table.
        if (optionBranch != null
                && (!DEFAULT_MAIN_BRANCH.equals(optionBranch) || DEFAULT_MAIN_BRANCH.equals(requestedBranch))) {
            return optionBranch;
        }
        return requestedBranch;
    }

    private Optional<Snapshot> resolveIndexSnapshot(FileStoreTable table, TvrVersionRange versionRange) {
        try {
            if (versionRange != null && versionRange.end().isPresent()) {
                long snapshotId = versionRange.end().get();
                if (snapshotId < 0) {
                    return Optional.empty();
                }
                try {
                    return Optional.of(table.snapshotManager().tryGetSnapshot(snapshotId));
                } catch (FileNotFoundException e) {
                    // An expired snapshot can still be retained by a tag after its ordinary
                    // snapshot file has been removed.
                    if (table instanceof DataTable) {
                        return ((DataTable) table).tagManager().taggedSnapshots().stream()
                                .filter(taggedSnapshot -> taggedSnapshot.id() == snapshotId)
                                .findFirst();
                    }
                    return Optional.empty();
                }
            }
            return Optional.ofNullable(table.snapshotManager().latestSnapshot());
        } catch (Exception e) {
            if (indexMetadataCache.shouldLogFailure(System.currentTimeMillis())) {
                LOG.warn("Cannot resolve Paimon snapshot for index metadata: {}", e.getMessage());
            }
            return Optional.empty();
        }
    }

    private static String tableBranchKey(PaimonTable table) {
        return table.getCatalogDBName() + "." + table.getCatalogTableName();
    }

    private static String tableBranchKey(org.apache.paimon.table.Table table) {
        String[] names = table.fullName().split("\\.");
        return names.length < 2 ? table.fullName() : names[names.length - 2] + "." + names[names.length - 1];
    }

    private static ConnectorIndexMetadata bindCurrentSchema(
            ConnectorIndexMetadata metadata, FileStoreTable table, Set<Integer> stableBtreeFields) {
        Map<String, Integer> currentFields = table.rowType().getFields().stream()
                .collect(Collectors.toMap(DataField::name, DataField::id));
        Map<Integer, String> currentFieldNames = currentFields.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        Set<Integer> currentFieldIds = new HashSet<>(currentFields.values());
        Set<Integer> partitionFields = table.partitionKeys().stream()
                .map(currentFields::get)
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toSet());
        List<ConnectorIndexDescriptor> currentDescriptors = metadata.getDescriptors().stream()
                .filter(descriptor -> currentFieldIds.contains(descriptor.getFieldId()))
                .filter(descriptor -> !"btree".equals(descriptor.getProvider())
                        || stableBtreeFields.contains(descriptor.getFieldId()))
                .map(descriptor -> descriptor.withColumnName(currentFieldNames.get(descriptor.getFieldId())))
                .collect(Collectors.toList());
        return ConnectorIndexMetadata.of(metadata.getSnapshotId(), metadata.getTableType(),
                currentDescriptors, currentFields, partitionFields);
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
            PaimonSplitUtils.getDataSplit(split).ifPresent(dataSplit -> dataSplit.dataFiles()
                    .forEach(dataFileMeta -> resultedTableFilesSize.addAndGet(dataFileMeta.fileSize())));
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
                                         TvrVersionRange versionRange) {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "GetPaimonTableStatistics")) {
            if (!properties.enableGetTableStatsFromExternalMetadata()) {
                return StatisticsUtils.buildDefaultStatistics(columns.keySet());
            }

            Statistics.Builder builder = Statistics.builder()
                    .setStatsSource(Statistics.StatsSource.TABLE_METADATA);
            // Stage 0 must not touch indexes, including during optimizer statistics collection.
            // Stage 2 leaves index evaluation to BEs instead of repeating it in FE for estimates.
            boolean disableGlobalIndex = session.getSessionVariable().getPaimonGlobalIndexScanStage() != 1;
            if (!session.getSessionVariable().enablePaimonColumnStatistics()) {
                return defaultStatistics(columns, table, predicate, limit, versionRange, disableGlobalIndex);
            }
            org.apache.paimon.table.Table nativeTable = getNativeTable(((PaimonTable) table).getNativeTable(), versionRange);
            Optional<org.apache.paimon.stats.Statistics> statistics = nativeTable.statistics();
            if (!statistics.isPresent() || statistics.get().colStats() == null
                    || !statistics.get().mergedRecordCount().isPresent()) {
                return defaultStatistics(columns, table, predicate, limit, versionRange, disableGlobalIndex);
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
                                         long limit, TvrVersionRange versionRange, boolean disableGlobalIndex) {
        Statistics.Builder builder = Statistics.builder()
                .setStatsSource(Statistics.StatsSource.TABLE_METADATA);
        for (ColumnRefOperator columnRefOperator : columns.keySet()) {
            builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
        }
        List<String> fieldNames = columns.keySet().stream().map(ColumnRefOperator::getName).collect(Collectors.toList());
        GetRemoteFilesParams params =
                GetRemoteFilesParams.newBuilder().setPredicate(predicate)
                        .setTableVersionRange(versionRange).setFieldNames(fieldNames).setLimit(limit)
                        .setDisableGlobalIndex(disableGlobalIndex).build();
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

            builder.setAverageRowSize(colStats.avgLen().isPresent()
                    ? colStats.avgLen().getAsLong() : column.getType().getTypeSize());

            if (colStats.distinctCount().isPresent()) {
                builder.setDistinctValuesCount(colStats.distinctCount().getAsLong());
            } else {
                builder.setDistinctValuesCount(ConnectorNdvEstimator.typeNdv(
                        ConnectorNdvEstimator.fromStarRocksType(column.getType()), Math.max(1L, rowCount)));
            }
            builder.setType(ColumnStatistic.StatisticType.ESTIMATE);
            columnStatistic = builder.build();
        }

        if (columnStatistic == null) {
            ConnectorNdvEstimator.TypeCategory cat = ConnectorNdvEstimator.fromStarRocksType(column.getType());
            columnStatistic = ColumnStatistic.builder()
                    .setDistinctValuesCount(ConnectorNdvEstimator.typeNdv(cat, Math.max(1L, rowCount)))
                    .setAverageRowSize(column.getType().getTypeSize())
                    .setNullsFraction(0)
                    .setType(ColumnStatistic.StatisticType.ESTIMATE)
                    .build();
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
    public void truncateTable(TruncateTableStmt truncateTableStmt, ConnectContext context) {
        String dbName = truncateTableStmt.getDbName();
        String tableName = truncateTableStmt.getTblName();
        Identifier identifier = new Identifier(dbName, tableName);

        org.apache.paimon.table.Table paimonTable;
        try {
            paimonTable = paimonNativeCatalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            throw new StarRocksConnectorException("Failed to truncate paimon table: %s.%s, table does not exist",
                    dbName, tableName);
        }

        String user = context.getCurrentUserIdentity() != null
                ? context.getCurrentUserIdentity().getUser() : "None";

        LOG.info("Truncating paimon table: {}.{}, triggered by user: {}", dbName, tableName, user);

        try (BatchTableCommit commit = paimonTable.newBatchWriteBuilder().newCommit()) {
            if (truncateTableStmt instanceof TruncateTablePartitionStmt partitionStmt) {
                if (paimonTable.partitionKeys().isEmpty()) {
                    throw new StarRocksConnectorException("Table [%s.%s] is not partitioned, " +
                            "cannot truncate partitions", dbName, tableName);
                }

                KeyPartitionRef partitionRef = partitionStmt.getKeyPartitionRef();
                List<String> partitionColNames = partitionRef.getPartitionColNames();
                List<String> tablePartitionKeys = paimonTable.partitionKeys();
                
                if (partitionColNames.stream().anyMatch(p -> !tablePartitionKeys.contains(p))) {
                    throw new StarRocksConnectorException("Partition names in partition spec do not match " +
                            "table partition columns for table [%s.%s]", dbName, tableName);
                }

                Map<String, String> partitionMap = buildPartitionMap(partitionRef);

                if (partitionMap.isEmpty()) {
                    throw new StarRocksConnectorException("Partition specification is empty for truncate operation");
                }

                commit.truncatePartitions(Collections.singletonList(partitionMap));
                LOG.info("Successfully truncated partitions of paimon table: {}.{}, partitions: {}, user: {}",
                        dbName, tableName, partitionMap, user);
            } else {
                commit.truncateTable();
                LOG.info("Successfully truncated paimon table: {}.{}, user: {}", dbName, tableName, user);
            }
        } catch (StarRocksConnectorException e) {
            LOG.error("Failed to truncate paimon table: {}.{}", dbName, tableName, e);
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to truncate paimon table: {}.{}", dbName, tableName, e);
            throw new StarRocksConnectorException("Failed to truncate paimon table: %s.%s, error: %s",
                    dbName, tableName, e.getMessage());
        }

        indexMetadataCache.invalidateTable(catalogName, dbName, tableName);

        Table table = tables.get(identifier);
        if (table != null) {
            refreshTable(dbName, table, null, true);
        }
    }

    private Map<String, String> buildPartitionMap(KeyPartitionRef partitionRef) {
        Map<String, String> partitionMap = new HashMap<>();
        List<String> partitionColNames = partitionRef.getPartitionColNames();
        List<Expr> partitionColValues = partitionRef.getPartitionColValues();

        for (int i = 0; i < partitionColNames.size(); i++) {
            String colName = partitionColNames.get(i);
            Expr valueExpr = partitionColValues.get(i);

            if (!(valueExpr instanceof LiteralExpr)) {
                throw new StarRocksConnectorException(
                        "Partition value must be a literal expression, got: %s",
                        valueExpr.getClass().getSimpleName());
            }

            String colValue = ((LiteralExpr) valueExpr).getStringValue();
            partitionMap.put(colName, colValue);
        }

        return partitionMap;
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        PaimonTable paimonTable = (PaimonTable) table;
        Identifier identifier = new Identifier(paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName());
        List<PartitionInfo> result = new ArrayList<>();
        if (table.isUnPartitioned()) {

            result.add(new Partition(paimonTable.getCatalogTableName(),
                    this.getTableUpdateTime(paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName()), null,
                    null, null));
            return result;
        }
        Map<String, Partition> partitionInfo = this.partitionInfos.getOrDefault(identifier, Collections.emptyMap());
        // Refresh before collecting results so a later cache miss also updates earlier hits.
        for (String partitionName : partitionNames) {
            if (!partitionInfo.containsKey(partitionName)) {
                this.updateAllPartitionInfos(paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName());
                partitionInfo = this.partitionInfos.getOrDefault(identifier, Collections.emptyMap());
                break;
            }
        }
        for (String partitionName : partitionNames) {
            Partition partition = partitionInfo.get(partitionName);
            if (partition != null) {
                result.add(partition);
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
        indexMetadataCache.invalidateTable(catalogName, srDbName, tableName);
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
