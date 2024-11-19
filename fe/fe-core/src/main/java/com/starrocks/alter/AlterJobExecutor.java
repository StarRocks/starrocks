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
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.persist.AlterViewInfo;
import com.starrocks.persist.SwapTableOperationLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
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
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class AlterJobExecutor extends AstVisitor<Void, ConnectContext> {
    protected static final Logger LOG = LogManager.getLogger(AlterJobExecutor.class);
    protected Database db;
    protected Table table;

    public AlterJobExecutor() {

    }

    public void process(StatementBase statement, ConnectContext context) {
        visit(statement, context);
    }

    //Alter system clause

    @Override
    public Void visitNode(ParseNode node, ConnectContext context) {
        throw new AlterJobException("Not support alter table operation : " + node.getClass().getName());
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
    public Void visitTableRenameClause(TableRenameClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitAlterTableCommentClause(AlterTableCommentClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitSwapTableClause(SwapTableClause clause, ConnectContext context) {
        // must hold db write lock
        Preconditions.checkState(db.isWriteLockHeldByCurrentThread());

        OlapTable origTable = (OlapTable) table;

        String origTblName = origTable.getName();
        String newTblName = clause.getTblName();
        Table newTbl = db.getTable(newTblName);
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

            // inactive the related MVs
            LocalMetastore.inactiveRelatedMaterializedView(db, origTable,
                    MaterializedViewExceptions.inactiveReasonForBaseTableSwapped(origTblName));
            LocalMetastore.inactiveRelatedMaterializedView(db, olapNewTbl,
                    MaterializedViewExceptions.inactiveReasonForBaseTableSwapped(newTblName));

            SwapTableOperationLog log = new SwapTableOperationLog(db.getId(), origTable.getId(), olapNewTbl.getId());
            GlobalStateMgr.getCurrentState().getAlterJobMgr().swapTableInternal(log);
            GlobalStateMgr.getCurrentState().getEditLog().logSwapTable(log);

            LOG.info("finish swap table {}-{} with table {}-{}", origTable.getId(), origTblName, newTbl.getId(),
                    newTblName);
            return null;
        } catch (DdlException e) {
            throw new AlterJobException(e.getMessage(), e);
        }
    }

    @Override
    public Void visitModifyTablePropertiesClause(ModifyTablePropertiesClause clause, ConnectContext context) {
<<<<<<< HEAD
        unsupportedException("Not support");
=======
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
        } catch (UserException e) {
            throw new AlterJobException(e.getMessage(), e);
        }
>>>>>>> 49f6f36538 ([BugFix] Fix disable base compaction with minute granularity & fe/be recover (#52923))
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
        unsupportedException("Not support");
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
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitCompactionClause(CompactionClause clause, ConnectContext context) {
        unsupportedException("Not support");
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
    public Void visitModifyPartitionClause(ModifyPartitionClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitAddPartitionClause(AddPartitionClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitDropPartitionClause(DropPartitionClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitTruncatePartitionClause(TruncatePartitionClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitReplacePartitionClause(ReplacePartitionClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    @Override
    public Void visitPartitionRenameClause(PartitionRenameClause clause, ConnectContext context) {
        unsupportedException("Not support");
        return null;
    }

    // Alter View
    @Override
    public Void visitAlterViewClause(AlterViewClause alterViewClause, ConnectContext ctx) {
        AlterViewInfo alterViewInfo = new AlterViewInfo(db.getId(), table.getId(),
                alterViewClause.getInlineViewDef(),
                alterViewClause.getColumns(),
                ctx.getSessionVariable().getSqlMode(), alterViewClause.getComment());

        GlobalStateMgr.getCurrentState().getAlterJobMgr().alterView(alterViewInfo);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyViewDef(alterViewInfo);
        return null;
    }

    @Override
    public Void visitAlterTableStatement(AlterTableStmt statement, ConnectContext context) {
        TableName tableName = statement.getTbl();
        Database db = MetaUtils.getDatabase(context, tableName);
        Table table = MetaUtils.getTable(tableName);

        if (table.getType() == Table.TableType.VIEW || table.getType() == Table.TableType.MATERIALIZED_VIEW) {
            throw new SemanticException("The specified table [" + tableName + "] is not a table");
        }

        this.db = db;
        this.table = table;
        for (AlterClause alterClause : statement.getOps()) {
            visit(alterClause, context);
        }
        return null;
    }

    @Override
    public Void visitAlterViewStatement(AlterViewStmt statement, ConnectContext context) {
        TableName tableName = statement.getTableName();
        Database db = MetaUtils.getDatabase(context, tableName);
        Table table = MetaUtils.getTable(tableName);

        if (table.getType() != Table.TableType.VIEW) {
            throw new SemanticException("The specified table [" + tableName + "] is not a view");
        }

        this.db = db;
        this.table = table;
        AlterViewClause alterViewClause = (AlterViewClause) statement.getAlterClause();
        visit(alterViewClause, context);
        return null;
    }

    @Override
    public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStmt stmt, ConnectContext context) {
        // check db
        final TableName mvName = stmt.getMvName();
        Database db = MetaUtils.getDatabase(context, mvName);

        if (!db.writeLockAndCheckExist()) {
            throw new AlterJobException("alter materialized failed. database:" + db.getFullName() + " not exist");
        }

        try {
            Table table = MetaUtils.getTable(mvName);
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
            db.writeUnlock();
        }
    }
}
