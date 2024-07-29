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

import com.google.common.collect.Lists;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.InvalidOlapTableStateException;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
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
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class AlterJobExecutor implements AstVisitor<Void, ConnectContext> {
    protected static final Logger LOG = LogManager.getLogger(AlterJobExecutor.class);
    protected TableName tableName;
    protected Database db;
    protected Table table;

    public AlterJobExecutor() {

    }

    public void process(StatementBase statement, ConnectContext context) {
        visit(statement, context);

        if (table instanceof OlapTable) {
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

        Database db = MetaUtils.getDatabase(context, tableName);
        Table table = MetaUtils.getTable(tableName);

        if (table.getType() == Table.TableType.VIEW || table.getType() == Table.TableType.MATERIALIZED_VIEW) {
            throw new SemanticException("The specified table [" + tableName + "] is not a table");
        }

        if (table instanceof OlapTable && ((OlapTable) table).getState() != OlapTable.OlapTableState.NORMAL) {
            OlapTable olapTable = (OlapTable) table;
            throw new AlterJobException("", InvalidOlapTableStateException.of(olapTable.getState(), olapTable.getName()));
        }

        this.db = db;
        this.table = table;

        for (AlterClause alterClause : statement.getAlterClauseList()) {
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

        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, LockType.WRITE)) {
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
            locker.unLockDatabase(db, LockType.WRITE);
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
    public Void visitTableRenameClause(TableRenameClause clause, ConnectContext context) {
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            ErrorReport.wrapWithRuntimeException(() ->
                    GlobalStateMgr.getCurrentState().getLocalMetastore().renameTable(db, table, clause));
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.WRITE);
        }
        return null;
    }

    @Override
    public Void visitAlterTableCommentClause(AlterTableCommentClause clause, ConnectContext context) {
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            ErrorReport.wrapWithRuntimeException(() -> GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .alterTableComment(db, table, clause));
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.WRITE);
        }
        return null;
    }

    @Override
    public Void visitSwapTableClause(SwapTableClause clause, ConnectContext context) {
        // must hold db write lock
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
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
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.WRITE);
        }
    }

    @Override
    public Void visitModifyTablePropertiesClause(ModifyTablePropertiesClause clause, ConnectContext context) {
        unsupportedException("Not support");
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
        locker.lockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            Set<String> modifiedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            modifiedColumns.add(clause.getColName());
            ErrorReport.wrapWithRuntimeException(() ->
                    schemaChangeHandler.checkModifiedColumWithMaterializedViews((OlapTable) table, modifiedColumns));
            GlobalStateMgr.getCurrentState().getLocalMetastore().renameColumn(db, table, clause);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.WRITE);
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
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            ErrorReport.wrapWithRuntimeException(() ->
                    GlobalStateMgr.getCurrentState().getLocalMetastore().renameRollup(db, (OlapTable) table, clause));
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.WRITE);
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

        ErrorReport.wrapWithRuntimeException(() ->
                GlobalStateMgr.getCurrentState().getLocalMetastore().dropPartition(db, table, clause));
        return null;
    }

    @Override
    public Void visitTruncatePartitionClause(TruncatePartitionClause clause, ConnectContext context) {
        // This logic is used to adapt mysql syntax.
        // ALTER TABLE test TRUNCATE PARTITION p1;
        TableRef tableRef = new TableRef(tableName, null, clause.getPartitionNames());
        TruncateTableStmt tStmt = new TruncateTableStmt(tableRef);
        ConnectContext ctx = new ConnectContext();
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
        locker.lockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            if (clause.getPartitionName().startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                throw new AlterJobException("Rename of shadow partitions is not allowed");
            }

            ErrorReport.wrapWithRuntimeException(() ->
                    GlobalStateMgr.getCurrentState().getLocalMetastore().renamePartition(db, table, clause));
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.WRITE);
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
            locker.lockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.WRITE);
            try {
                AlterJobMgr alterJobMgr = GlobalStateMgr.getCurrentState().getAlterJobMgr();
                alterJobMgr.modifyPartitionsProperty(db, (OlapTable) table, partitionNames, properties);
            } finally {
                locker.unLockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.WRITE);
            }
        } catch (DdlException | AnalysisException e) {
            throw new AlterJobException(e.getMessage());
        }
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
}
