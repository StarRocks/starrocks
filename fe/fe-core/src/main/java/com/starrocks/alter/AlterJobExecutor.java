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

<<<<<<< HEAD
=======
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

>>>>>>> d44ae38108 ([BugFix] Fix alter table partition (*) set storage_cooldown_ttl error (#52482))
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
