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

package com.starrocks.alter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.InvalidOlapTableStateException;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.AlterViewInfo;
import com.starrocks.persist.BatchModifyPartitionsInfo;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.SwapTableOperationLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddFieldClause;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewClause;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.CompactionClause;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropFieldClause;
import com.starrocks.sql.ast.DropIndexClause;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropPersistentIndexClause;
import com.starrocks.sql.ast.DropRollupClause;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyPartitionClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.OptimizeClause;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.ReorderColumnsClause;
import com.starrocks.sql.ast.ReplacePartitionClause;
import com.starrocks.sql.ast.RollupRenameClause;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SwapTableClause;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.TruncatePartitionClause;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTabletType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.threeten.extra.PeriodDuration;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class AlterJobExecutor implements AstVisitor<Void, ConnectContext> {
    protected static final Logger LOG = LogManager.getLogger(AlterJobExecutor.class);
    protected TableName tableName;
    protected Database db;
    protected Table table;
    private boolean isSynchronous;

    public AlterJobExecutor() {
        isSynchronous = true;
    }

    public void process(StatementBase statement, ConnectContext context) {
        visit(statement, context);

        if (isSynchronous && table instanceof OlapTable) {
            ((OlapTable) table).lastSchemaUpdateTime.set(System.nanoTime());
        }
    }

    @Override
    public Void visitNode(ParseNode node, ConnectContext context) {
        throw new AlterJobException("Not support alter table operation : " + node.getClass().getName());
    }

    @Override
    public Void visitAlterTableStatement(AlterTableStmt statement, ConnectContext context) {
        TableName tableName = statement.getTbl();
        this.tableName = tableName;

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(tableName.getDb());
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(tableName.getDb(), tableName.getTbl());
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableName);
        }

        if (!(table.isOlapOrCloudNativeTable() || table.isMaterializedView())) {
            throw new SemanticException("Do not support alter non-native table/materialized-view[" + tableName + "]");
        }

        if (table instanceof OlapTable && ((OlapTable) table).getState() != OlapTable.OlapTableState.NORMAL) {
            OlapTable olapTable = (OlapTable) table;
            throw new AlterJobException("", InvalidOlapTableStateException.of(olapTable.getState(), olapTable.getName()));
        }

        this.db = db;
        this.table = table;

        List<AlterOpType> alterOpTypes = statement.getAlterClauseList()
                .stream().map(AlterClause::getOpType).collect(Collectors.toList());

        if (alterOpTypes.contains(AlterOpType.SCHEMA_CHANGE) || alterOpTypes.contains(AlterOpType.OPTIMIZE)) {
            Locker locker = new Locker();
            locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
            try {
                SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
                assert table instanceof OlapTable;
                schemaChangeHandler.process(statement.getAlterClauseList(), db, (OlapTable) table);
            } catch (StarRocksException e) {
                throw new AlterJobException(e.getMessage());
            } finally {
                locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
            }

            isSynchronous = false;
        } else if (alterOpTypes.contains(AlterOpType.ADD_ROLLUP) || alterOpTypes.contains(AlterOpType.DROP_ROLLUP)) {
            Locker locker = new Locker();
            locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
            try {
                MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getAlterJobMgr()
                        .getMaterializedViewHandler();
                Preconditions.checkState(table instanceof OlapTable);
                ErrorReport.wrapWithRuntimeException(() ->
                        materializedViewHandler.process(statement.getAlterClauseList(), db, (OlapTable) table));
            } finally {
                locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
            }

            isSynchronous = false;
        } else {
            for (AlterClause alterClause : statement.getAlterClauseList()) {
                visit(alterClause, context);
            }
        }
        return null;
    }

    @Override
    public Void visitAlterViewStatement(AlterViewStmt statement, ConnectContext context) {
        TableName tableName = statement.getTableName();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(tableName.getDb());
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(tableName.getDb(), tableName.getTbl());
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableName);
        }

        if (table.getType() != Table.TableType.VIEW) {
            throw new SemanticException("The specified table [" + tableName + "] is not a view");
        }

        this.db = db;
        this.table = table;

        if (statement.getAlterClause() == null) {
            ((View) table).setSecurity(statement.isSecurity());
            return null;
        }

        AlterViewClause alterViewClause = (AlterViewClause) statement.getAlterClause();
        visit(alterViewClause, context);
        return null;
    }

    @Override
    public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStmt stmt, ConnectContext context) {
        // check db
        final TableName mvName = stmt.getMvName();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not found", mvName.getCatalogAndDb());
        }

        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, LockType.WRITE)) {
            throw new AlterJobException("alter materialized failed. database:" + db.getFullName() + " not exist");
        }

        try {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(mvName.getDb(), mvName.getTbl());
            if (table == null) {
                throw new SemanticException("Table %s is not found", mvName);
            }
            if (!table.isMaterializedView()) {
                throw new SemanticException("The specified table [" + mvName + "] is not a view");
            }
            this.db = db;
            this.table = table;

            MaterializedView materializedView = (MaterializedView) table;
            // check materialized view state
            if (materializedView.getState() != OlapTable.OlapTableState.NORMAL) {
                throw new AlterJobException("Materialized view [" + materializedView.getName() + "]'s state is not NORMAL. "
                        + "Do not allow to do ALTER ops");
            }

            GlobalStateMgr.getCurrentState().getMaterializedViewMgr().stopMaintainMV(materializedView);
            visit(stmt.getAlterTableClause());
            GlobalStateMgr.getCurrentState().getMaterializedViewMgr().rebuildMaintainMV(materializedView);
            return null;
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    //Alter table clause

    @Override
    public Void visitCreateIndexClause(CreateIndexClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitDropIndexClause(DropIndexClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitDropPersistentIndexClause(DropPersistentIndexClause clause, ConnectContext context) {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        try {
            schemaChangeHandler.processLakeTableDropPersistentIndex(clause, db, (OlapTable) table);
        } catch (StarRocksException e) {
            throw new AlterJobException(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public Void visitTableRenameClause(TableRenameClause clause, ConnectContext context) {
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            ErrorReport.wrapWithRuntimeException(() ->
                    GlobalStateMgr.getCurrentState().getLocalMetastore().renameTable(db, table, clause));
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        }
        return null;
    }

    @Override
    public Void visitAlterTableCommentClause(AlterTableCommentClause clause, ConnectContext context) {
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            ErrorReport.wrapWithRuntimeException(() -> GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .alterTableComment(db, table, clause));
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        }
        return null;
    }

    @Override
    public Void visitSwapTableClause(SwapTableClause clause, ConnectContext context) {
        // must hold db write lock
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            OlapTable origTable = (OlapTable) table;
            String origTblName = origTable.getName();
            String newTblName = clause.getTblName();
            Table newTbl = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), newTblName);
            if (newTbl == null || !(newTbl.isOlapOrCloudNativeTable() || newTbl.isMaterializedView())) {
                throw new AlterJobException("Table " + newTblName + " does not exist or is not OLAP/LAKE table");
            }
            OlapTable olapNewTbl = (OlapTable) newTbl;

            // First, we need to check whether the table to be operated on can be renamed
            try {
                olapNewTbl.checkAndSetName(origTblName, true);
                origTable.checkAndSetName(newTblName, true);

                if (origTable.isMaterializedView() || newTbl.isMaterializedView()) {
                    if (!(origTable.isMaterializedView() && newTbl.isMaterializedView())) {
                        throw new AlterJobException("Materialized view can only SWAP WITH materialized view");
                    }
                }

                // check unique constraints: if old table contains constraints, new table must contain the same constraints
                final List<UniqueConstraint> origUKConstraints = Optional.ofNullable(origTable.getUniqueConstraints())
                        .orElse(Lists.newArrayList());
                final List<UniqueConstraint> newUKConstraints = Optional.ofNullable(olapNewTbl.getUniqueConstraints())
                        .orElse(Lists.newArrayList());
                if (origUKConstraints.size() != newUKConstraints.size()) {
                    throw new AlterJobException("Table " + newTblName + " does not contain the same unique constraints, " +
                            "origin: " + origUKConstraints + ", new: " + newUKConstraints);
                }
                for (UniqueConstraint origUniqueConstraint : origUKConstraints) {
                    final List<String> originUKNames = origUniqueConstraint.getUniqueColumnNames(origTable);
                    final List<String> newUKNames = origUniqueConstraint.getUniqueColumnNames(olapNewTbl);
                    if (originUKNames.size() != newUKNames.size()) {
                        throw new AlterJobException("Table " + newTblName + " does not contain the same unique constraints" +
                                ", origin: " + origUKConstraints + ", new: " + newUKConstraints);
                    }
                    for (int i = 0; i < originUKNames.size(); i++) {
                        if (!originUKNames.get(i).equals(newUKNames.get(i))) {
                            throw new AlterJobException("Table " + newTblName + " does not contain the same unique constraints" +
                                    ", origin: " + origUKConstraints + ", new: " + newUKConstraints);
                        }
                    }
                }
                // check foreign constraints: if old table contains constraints, new table must contain the same constraints
                final List<ForeignKeyConstraint> origFKConstraints = Optional.ofNullable(origTable.getForeignKeyConstraints())
                        .orElse(Lists.newArrayList());
                final List<ForeignKeyConstraint> newFKConstraints = Optional.ofNullable(olapNewTbl.getForeignKeyConstraints())
                        .orElse(Lists.newArrayList());
                if (origFKConstraints.size() != newFKConstraints.size()) {
                    throw new AlterJobException("Table " + newTblName + " does not contain the same foreign key " +
                            "constraints, origin: " + origFKConstraints + ", new: " + newFKConstraints);
                }
                for (int i = 0; i < origFKConstraints.size(); i++) {
                    final ForeignKeyConstraint originFKConstraint = origFKConstraints.get(i);
                    final ForeignKeyConstraint newFKConstraint = newFKConstraints.get(i);
                    final List<Pair<String, String>> originFKCNPairs = originFKConstraint.getColumnNameRefPairs(origTable);
                    final List<Pair<String, String>> newFKCNPairs = newFKConstraint.getColumnNameRefPairs(origTable);
                    if (originFKCNPairs.size() != newFKCNPairs.size()) {
                        throw new AlterJobException("Table " + newTblName + " does not contain the same foreign key " +
                                "constraints, origin: " + origFKConstraints + ", new: " + newFKConstraints);
                    }
                    for (int j = 0; j < originFKCNPairs.size(); j++) {
                        final Pair<String, String> originPair = originFKCNPairs.get(j);
                        final Pair<String, String> newPair = newFKCNPairs.get(j);
                        if (!originPair.first.equals(newPair.first) || !originPair.second.equals(newPair.second)) {
                            throw new AlterJobException("Table " + newTblName + " does not contain the same foreign key " +
                                    "constraints, origin: " + origFKConstraints + ", new: " + newFKConstraints);
                        }
                    }
                }

                // inactive the related MVs
                AlterMVJobExecutor.inactiveRelatedMaterializedView(origTable,
                        MaterializedViewExceptions.inactiveReasonForBaseTableSwapped(origTblName), false);
                AlterMVJobExecutor.inactiveRelatedMaterializedView(olapNewTbl,
                        MaterializedViewExceptions.inactiveReasonForBaseTableSwapped(newTblName), false);

                SwapTableOperationLog log = new SwapTableOperationLog(db.getId(), origTable.getId(), olapNewTbl.getId());
                GlobalStateMgr.getCurrentState().getAlterJobMgr().swapTableInternal(log);
                GlobalStateMgr.getCurrentState().getEditLog().logSwapTable(log);

                LOG.info("finish swap table {}-{} with table {}-{}", origTable.getId(), origTblName, newTbl.getId(),
                        newTblName);
                return null;
            } catch (DdlException e) {
                throw new AlterJobException(e.getMessage(), e);
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        }
    }

    @Override
    public Void visitModifyTablePropertiesClause(ModifyTablePropertiesClause clause, ConnectContext context) {
        try {
            Map<String, String> properties = clause.getProperties();
            SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM)) {
                schemaChangeHandler.updateTableMeta(db, tableName.getTbl(), properties, TTabletMetaType.WRITE_QUORUM);
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
                schemaChangeHandler.updateTableMeta(db, tableName.getTbl(), properties, TTabletMetaType.INMEMORY);
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC)) {
                schemaChangeHandler.updateTableMeta(db, tableName.getTbl(), properties,
                        TTabletMetaType.PRIMARY_INDEX_CACHE_EXPIRE_SEC);
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX)
                    || properties.containsKey(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE)) {
                if (table.isCloudNativeTable()) {
                    Locker locker = new Locker();
                    locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
                    try {
                        schemaChangeHandler.processLakeTableAlterMeta(clause, db, (OlapTable) table);
                    } finally {
                        locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
                    }

                    isSynchronous = false;
                } else {
                    if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE)) {
                        throw new DdlException("StarRocks doesn't support alter persistent_index_type under shared-nothing mode");
                    }
                    schemaChangeHandler.updateTableMeta(db, tableName.getTbl(), properties,
                            TTabletMetaType.ENABLE_PERSISTENT_INDEX);
                }
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE)) {
                schemaChangeHandler.updateTableMeta(db, tableName.getTbl(), properties,
                        TTabletMetaType.REPLICATED_STORAGE);
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BUCKET_SIZE)) {
                schemaChangeHandler.updateTableMeta(db, tableName.getTbl(), properties,
                        TTabletMetaType.BUCKET_SIZE);
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_MUTABLE_BUCKET_NUM)) {
                schemaChangeHandler.updateTableMeta(db, tableName.getTbl(), properties,
                        TTabletMetaType.MUTABLE_BUCKET_NUM);
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_LOAD_PROFILE)) {
                schemaChangeHandler.updateTableMeta(db, tableName.getTbl(), properties,
                        TTabletMetaType.ENABLE_LOAD_PROFILE);
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BASE_COMPACTION_FORBIDDEN_TIME_RANGES)) {
                try {
                    GlobalStateMgr.getCurrentState().getCompactionControlScheduler().updateTableForbiddenTimeRanges(
                            table.getId(), properties.get(PropertyAnalyzer.PROPERTIES_BASE_COMPACTION_FORBIDDEN_TIME_RANGES));
                    schemaChangeHandler.updateTableMeta(db, tableName.getTbl(), properties,
                            TTabletMetaType.BASE_COMPACTION_FORBIDDEN_TIME_RANGES);
                } catch (Exception e) {
                    LOG.warn("Failed to update base compaction forbidden time ranges: " + tableName.getTbl(), e);
                    throw new DdlException("Failed to update base compaction forbidden time ranges for "
                            + tableName.getTbl() + ": " + e.getMessage());
                }
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE) ||
                    properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL) ||
                    properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE)) {
                boolean isSuccess = schemaChangeHandler.updateBinlogConfigMeta(db, table.getId(),
                        properties, TTabletMetaType.BINLOG_CONFIG);
                if (!isSuccess) {
                    throw new DdlException("modify binlog config of FEMeta failed or table has been droped");
                }
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)
                    || properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)) {
                schemaChangeHandler.updateTableConstraint(db, tableName.getTbl(), properties);
            } else {
                Locker locker = new Locker();
                locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
                try {
                    OlapTable olapTable = (OlapTable) table;
                    if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)) {
                        if (olapTable.getLocation() != null) {
                            ErrorReport.reportDdlException(
                                    ErrorCode.ERR_LOC_AWARE_UNSUPPORTED_FOR_COLOCATE_TBL, olapTable.getName());
                        }
                        String colocateGroup = properties.get(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH);
                        GlobalStateMgr.getCurrentState().getColocateTableIndex()
                                .modifyTableColocate(db, olapTable, colocateGroup, false, null);
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE)) {
                        GlobalStateMgr.getCurrentState().getLocalMetastore().convertDistributionType(db, olapTable);
                    } else if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(properties)) {
                        if (!olapTable.dynamicPartitionExists()) {
                            try {
                                DynamicPartitionUtil.checkInputDynamicPartitionProperties(
                                        olapTable, properties, olapTable.getPartitionInfo());
                            } catch (DdlException e) {
                                // This table is not a dynamic partition table and didn't supply all dynamic partition properties
                                throw new DdlException("Table " + db.getOriginName() + "." +
                                        olapTable.getName() + " is not a dynamic partition table.");
                            }
                        }
                        if (properties.containsKey(DynamicPartitionProperty.BUCKETS)) {
                            String colocateGroup = olapTable.getColocateGroup();
                            if (colocateGroup != null) {
                                throw new DdlException("The table has a colocate group:" + colocateGroup + ". so cannot " +
                                        "modify dynamic_partition.buckets. Colocate tables must have same bucket number.");
                            }
                        }
                        GlobalStateMgr.getCurrentState().getLocalMetastore()
                                .modifyTableDynamicPartition(db, olapTable, properties);
                    } else if (properties.containsKey("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
                        Preconditions.checkNotNull(properties.get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
                        GlobalStateMgr.getCurrentState().getLocalMetastore()
                                .modifyTableDefaultReplicationNum(db, olapTable, properties);
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
                        GlobalStateMgr.getCurrentState().getLocalMetastore().modifyTableReplicationNum(db, olapTable, properties);
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER)) {
                        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTableProperties(db, olapTable, properties);
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL)) {
                        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTableProperties(db, olapTable, properties);
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION)) {
                        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTableProperties(db, olapTable, properties);
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
                        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTableProperties(db, olapTable, properties);
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL)) {
                        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTableProperties(db, olapTable, properties);
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION)) {
                        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTableProperties(db, olapTable, properties);
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION)) {
                        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTableProperties(db, olapTable, properties);
                    } else {
                        schemaChangeHandler.process(Lists.newArrayList(clause), db, olapTable);
                    }
                } finally {
                    locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
                }

                isSynchronous = false;
            }
        } catch (StarRocksException e) {
            throw new AlterJobException(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public Void visitOptimizeClause(OptimizeClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitAddColumnClause(AddColumnClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitAddColumnsClause(AddColumnsClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitDropColumnClause(DropColumnClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitModifyColumnClause(ModifyColumnClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitColumnRenameClause(ColumnRenameClause clause, ConnectContext context) {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            Set<String> modifiedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            modifiedColumns.add(clause.getColName());
            ErrorReport.wrapWithRuntimeException(() ->
                    schemaChangeHandler.checkModifiedColumWithMaterializedViews((OlapTable) table, modifiedColumns));
            GlobalStateMgr.getCurrentState().getLocalMetastore().renameColumn(db, table, clause);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        }
        return null;
    }

    @Override
    public Void visitReorderColumnsClause(ReorderColumnsClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitAddRollupClause(AddRollupClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitDropRollupClause(DropRollupClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitRollupRenameClause(RollupRenameClause clause, ConnectContext context) {
        try (AutoCloseableLock ignore =
                    new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE)) {
            ErrorReport.wrapWithRuntimeException(() ->
                    GlobalStateMgr.getCurrentState().getLocalMetastore().renameRollup(db, (OlapTable) table, clause));
        }

        return null;
    }

    @Override
    public Void visitCompactionClause(CompactionClause clause, ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                CompactionHandler.process(Lists.newArrayList(clause), db, (OlapTable) table));
        return null;
    }

    @Override
    public Void visitAddFieldClause(AddFieldClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitDropFieldClause(DropFieldClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    //Alter partition clause

    @Override
    public Void visitAddPartitionClause(AddPartitionClause clause, ConnectContext context) {
        /*
         * This check is not appropriate here. However, because the dynamically generated Partition Clause needs to be analyzed,
         * this check cannot be placed in analyze.
         * For the same reason, it cannot be placed in LocalMetastore.addPartition.
         * If there is a subsequent refactoring, this check logic should be placed in a more appropriate location.
         */
        if (!clause.isTempPartition() && table instanceof OlapTable) {
            DynamicPartitionUtil.checkAlterAllowed((OlapTable) table);
        }
        ErrorReport.wrapWithRuntimeException(() ->
                GlobalStateMgr.getCurrentState().getLocalMetastore().addPartitions(context, db, table.getName(), clause));
        return null;
    }

    @Override
    public Void visitDropPartitionClause(DropPartitionClause clause, ConnectContext context) {
        /*
         * This check is not appropriate here. However, because the dynamically generated Partition Clause needs to be analyzed,
         * this check cannot be placed in analyze.
         * For the same reason, it cannot be placed in LocalMetastore.dropPartition.
         * If there is a subsequent refactoring, this check logic should be placed in a more appropriate location.
         */
        if (!clause.isTempPartition() && table instanceof OlapTable) {
            DynamicPartitionUtil.checkAlterAllowed((OlapTable) table);
        }

        try (AutoCloseableLock ignore =
                    new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE)) {
            ErrorReport.wrapWithRuntimeException(() ->
                    GlobalStateMgr.getCurrentState().getLocalMetastore().dropPartition(db, table, clause));
        }

        return null;
    }

    @Override
    public Void visitTruncatePartitionClause(TruncatePartitionClause clause, ConnectContext context) {
        // This logic is used to adapt mysql syntax.
        // ALTER TABLE test TRUNCATE PARTITION p1;
        TableRef tableRef = new TableRef(tableName, null, clause.getPartitionNames());
        TruncateTableStmt tStmt = new TruncateTableStmt(tableRef);
        ConnectContext ctx = ConnectContext.buildInner();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        ErrorReport.wrapWithRuntimeException(() ->
                GlobalStateMgr.getCurrentState().getLocalMetastore().truncateTable(tStmt, ctx));
        return null;
    }

    @Override
    public Void visitReplacePartitionClause(ReplacePartitionClause clause, ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                GlobalStateMgr.getCurrentState().getLocalMetastore().replaceTempPartition(db, table.getName(), clause));
        return null;
    }

    @Override
    public Void visitPartitionRenameClause(PartitionRenameClause clause, ConnectContext context) {
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            if (clause.getPartitionName().startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                throw new AlterJobException("Rename of shadow partitions is not allowed");
            }

            ErrorReport.wrapWithRuntimeException(() ->
                    GlobalStateMgr.getCurrentState().getLocalMetastore().renamePartition(db, table, clause));
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        }
        return null;
    }

    @Override
    public Void visitModifyPartitionClause(ModifyPartitionClause clause, ConnectContext context) {
        try {
            // expand the partition names if it is 'Modify Partition(*)'
            List<String> partitionNames = clause.getPartitionNames();
            if (clause.isNeedExpand()) {
                partitionNames.clear();
                for (Partition partition : table.getPartitions()) {
                    partitionNames.add(partition.getName());
                }
            }
            Map<String, String> properties = clause.getProperties();
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
                if (table.isCloudNativeTable()) {
                    throw new SemanticException("Lake table not support alter in_memory");
                }

                SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
                schemaChangeHandler.updatePartitionsInMemoryMeta(
                        db, table.getName(), partitionNames, properties);
            }

            Locker locker = new Locker();
            locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
            try {
                modifyPartitionsProperty(db, (OlapTable) table, partitionNames, properties);
            } finally {
                locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
            }
        } catch (DdlException | AnalysisException e) {
            throw new AlterJobException(e.getMessage());
        }
        return null;
    }

    /**
     * Batch update partitions' properties
     * caller should hold the db lock
     */
    private void modifyPartitionsProperty(Database db,
                                          OlapTable olapTable,
                                          List<String> partitionNames,
                                          Map<String, String> properties)
            throws DdlException, AnalysisException {
        Locker locker = new Locker();
        Preconditions.checkArgument(locker.isDbWriteLockHeldByCurrentThread(db));
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        List<ModifyPartitionInfo> modifyPartitionInfos = Lists.newArrayList();
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw InvalidOlapTableStateException.of(olapTable.getState(), olapTable.getName());
        }

        for (String partitionName : partitionNames) {
            Partition partition = olapTable.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException(
                        "Partition[" + partitionName + "] does not exist in table[" + olapTable.getName() + "]");
            }
        }

        boolean hasInMemory = properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY);

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        // get value from properties here
        // 1. data property
        PeriodDuration periodDuration = null;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL)) {
            String storageCoolDownTTL = properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL);
            if (!partitionInfo.isRangePartition()) {
                throw new DdlException("Only support range partition table to modify storage_cooldown_ttl");
            }
            if (Strings.isNotBlank(storageCoolDownTTL)) {
                periodDuration = TimeUtils.parseHumanReadablePeriodOrDuration(storageCoolDownTTL);
            }
        }
        DataProperty newDataProperty =
                PropertyAnalyzer.analyzeDataProperty(properties, null, false);
        // 2. replication num
        short newReplicationNum =
                PropertyAnalyzer.analyzeReplicationNum(properties, (short) -1);
        // 3. in memory
        boolean newInMemory = PropertyAnalyzer.analyzeBooleanProp(properties,
                PropertyAnalyzer.PROPERTIES_INMEMORY, false);
        // 4. tablet type
        TTabletType tTabletType =
                PropertyAnalyzer.analyzeTabletType(properties);

        // modify meta here
        for (String partitionName : partitionNames) {
            Partition partition = olapTable.getPartition(partitionName);
            // 1. date property

            // skip change storage_cooldown_ttl for shadow partition
            if (partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)
                    && newDataProperty != null) {
                continue;
            }

            if (newDataProperty != null) {
                // for storage_cooldown_ttl
                if (periodDuration != null) {
                    RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                    Range<PartitionKey> range = rangePartitionInfo.getRange(partition.getId());
                    PartitionKey partitionKey = range.upperEndpoint();
                    if (partitionKey.isMaxValue()) {
                        newDataProperty = new DataProperty(TStorageMedium.SSD, DataProperty.MAX_COOLDOWN_TIME_MS);
                    } else {
                        String stringUpperValue = partitionKey.getKeys().get(0).getStringValue();
                        DateTimeFormatter dateTimeFormatter = DateUtils.probeFormat(stringUpperValue);
                        LocalDateTime upperTime = DateUtils.parseStringWithDefaultHSM(stringUpperValue, dateTimeFormatter);
                        LocalDateTime updatedUpperTime = upperTime.plus(periodDuration);
                        DateLiteral dateLiteral = new DateLiteral(updatedUpperTime, Type.DATETIME);
                        long coolDownTimeStamp = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
                        newDataProperty = new DataProperty(TStorageMedium.SSD, coolDownTimeStamp);
                    }
                }
                partitionInfo.setDataProperty(partition.getId(), newDataProperty);
            }
            // 2. replication num
            if (newReplicationNum != (short) -1) {
                if (colocateTableIndex.isColocateTable(olapTable.getId())) {
                    throw new DdlException(
                            "table " + olapTable.getName() + " is colocate table, cannot change replicationNum");
                }
                partitionInfo.setReplicationNum(partition.getId(), newReplicationNum);
                // update default replication num if this table is unpartitioned table
                if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                    olapTable.setReplicationNum(newReplicationNum);
                }
            }
            // 3. in memory
            boolean oldInMemory = partitionInfo.getIsInMemory(partition.getId());
            if (hasInMemory && (newInMemory != oldInMemory)) {
                partitionInfo.setIsInMemory(partition.getId(), newInMemory);
            }
            // 4. tablet type
            if (tTabletType != partitionInfo.getTabletType(partition.getId())) {
                partitionInfo.setTabletType(partition.getId(), tTabletType);
            }
            ModifyPartitionInfo info = new ModifyPartitionInfo(db.getId(), olapTable.getId(), partition.getId(),
                    newDataProperty, newReplicationNum, hasInMemory ? newInMemory : oldInMemory);
            modifyPartitionInfos.add(info);
        }

        // log here
        BatchModifyPartitionsInfo info = new BatchModifyPartitionsInfo(modifyPartitionInfos);
        GlobalStateMgr.getCurrentState().getEditLog().logBatchModifyPartition(info);
    }

    // Alter View
    @Override
    public Void visitAlterViewClause(AlterViewClause alterViewClause, ConnectContext ctx) {
        AlterViewInfo alterViewInfo = new AlterViewInfo(db.getId(), table.getId(),
                alterViewClause.getInlineViewDef(),
                alterViewClause.getColumns(),
                ctx.getSessionVariable().getSqlMode(), alterViewClause.getComment());

        GlobalStateMgr.getCurrentState().getAlterJobMgr().alterView(alterViewInfo, false);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyViewDef(alterViewInfo);
        return null;
    }
}
