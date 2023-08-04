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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/alter/SchemaChangeHandler.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.alter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.ColumnPosition;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.SlotRef;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.UniqueConstraint;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.NotImplementedException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.WriteQuorum;
import com.starrocks.persist.TableAddOrDropColumnsInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.CancelStmt;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropIndexClause;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.ReorderColumnsClause;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.ClearAlterTask;
import com.starrocks.task.UpdateTabletMetaInfoTask;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TWriteQuorumType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class SchemaChangeHandler extends AlterHandler {

    private static final Logger LOG = LogManager.getLogger(SchemaChangeHandler.class);

    // all shadow indexes should have this prefix in name
    public static final String SHADOW_NAME_PRFIX = "__starrocks_shadow_";
    // before version 1.18, use "__doris_shadow_" as shadow index prefix
    // check "__doris_shadow_" to prevent compatibility problems
    public static final String SHADOW_NAME_PRFIX_V1 = "__doris_shadow_";

    public SchemaChangeHandler() {
        super("schema change");
    }

    /**
     * @param alterClause
     * @param olapTable
     * @param indexSchemaMap
     * @param colUniqueIdSupplier for multi add columns clause, we need stash middle state of maxColUniqueId
     * @return true: can light schema change, false: cannot light schema change
     * @throws DdlException
     */
    private boolean processAddColumn(AddColumnClause alterClause, OlapTable olapTable,
            Map<Long, LinkedList<Column>> indexSchemaMap,
            IntSupplier colUniqueIdSupplier) throws DdlException {
        Column column = alterClause.getColumn();
        ColumnPosition columnPos = alterClause.getColPos();
        String targetIndexName = alterClause.getRollupName();
        checkIndexExists(olapTable, targetIndexName);

        String baseIndexName = olapTable.getName();
        checkAssignedTargetIndexName(baseIndexName, targetIndexName);

        long baseIndexId = olapTable.getBaseIndexId();
        long targetIndexId = -1L;
        if (targetIndexName != null) {
            targetIndexId = olapTable.getIndexIdByName(targetIndexName);
        }

        Set<String> newColNameSet = Sets.newHashSet(column.getName());
        //only new table generate ColUniqueId, exist table do not.
        if (olapTable.getMaxColUniqueId() > Column.COLUMN_UNIQUE_ID_INIT_VALUE) {
            column.setUniqueId(colUniqueIdSupplier.getAsInt());
        }

        return addColumnInternal(olapTable, column, columnPos, targetIndexId, baseIndexId, indexSchemaMap,
                newColNameSet);

    }

    /**
     * @param alterClause
     * @param olapTable
     * @param indexSchemaMap
     * @param colUniqueIdSupplier for multi add columns clause, we need stash middle state of maxColUniqueId
     * @return true: can light schema change, false: cannot light schema change
     * @throws DdlException
     */
    private boolean processAddColumns(AddColumnsClause alterClause, OlapTable olapTable,
            Map<Long, LinkedList<Column>> indexSchemaMap,
            IntSupplier colUniqueIdSupplier) throws DdlException {
        List<Column> columns = alterClause.getColumns();
        String targetIndexName = alterClause.getRollupName();
        checkIndexExists(olapTable, targetIndexName);

        Set<String> newColNameSet = Sets.newHashSet();
        for (Column column : columns) {
            newColNameSet.add(column.getName());
        }

        String baseIndexName = olapTable.getName();
        checkAssignedTargetIndexName(baseIndexName, targetIndexName);

        long baseIndexId = olapTable.getBaseIndexId();
        long targetIndexId = -1L;
        if (targetIndexName != null) {
            targetIndexId = olapTable.getIndexIdByName(targetIndexName);
        }

        //for new table calculate column unique id
        if (olapTable.getMaxColUniqueId() > Column.COLUMN_UNIQUE_ID_INIT_VALUE) {
            for (Column column : columns) {
                column.setUniqueId(colUniqueIdSupplier.getAsInt());
            }
        }

        boolean ligthSchemaChange = true;
        if (alterClause.getMaterializedColumnPos() == null) {
            for (Column column : columns) {
                ligthSchemaChange = addColumnInternal(olapTable, column, null, targetIndexId, baseIndexId, indexSchemaMap,
                        newColNameSet);
            }
        } else {
            for (int i = columns.size() - 1; i >= 0; --i) {
                Column column = columns.get(i);
                ligthSchemaChange = addColumnInternal(olapTable, column, alterClause.getMaterializedColumnPos(),
                        targetIndexId, baseIndexId, indexSchemaMap, newColNameSet);
            }
        }
        return ligthSchemaChange;
    }

    /**
     * @param alterClause
     * @param olapTable
     * @param indexSchemaMap
     * @param indexes
     * @return true: can light schema change, false: cannot
     * @throws DdlException
     */
    private boolean processDropColumn(DropColumnClause alterClause, OlapTable olapTable,
            Map<Long, LinkedList<Column>> indexSchemaMap, List<Index> indexes) throws DdlException {

        boolean lightSchemaChange = olapTable.getUseLightSchemaChange();
        String dropColName = alterClause.getColName();
        String targetIndexName = alterClause.getRollupName();
        checkIndexExists(olapTable, targetIndexName);

        String baseIndexName = olapTable.getName();
        checkAssignedTargetIndexName(baseIndexName, targetIndexName);

        /*
         * PRIMARY:
         *      Can not drop any key/sort column.
         * UNIQUE:
         *      Can not drop any key column.
         * AGGREGATION:
         *      Can not drop any key column is has value with REPLACE method
         */
        if (KeysType.PRIMARY_KEYS == olapTable.getKeysType()) {
            // not support drop column in primary model
            lightSchemaChange = false;
            long baseIndexId = olapTable.getBaseIndexId();
            List<Column> baseSchema = indexSchemaMap.get(baseIndexId);
            boolean isKey = baseSchema.stream().anyMatch(c -> c.isKey() && c.getName().equalsIgnoreCase(dropColName));
            if (isKey) {
                throw new DdlException("Can not drop key column in primary data model table");
            }
            MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(olapTable.getBaseIndexId());
            if (indexMeta.getSortKeyIdxes() != null) {
                for (Integer sortKeyIdx : indexMeta.getSortKeyIdxes()) {
                    if (indexMeta.getSchema().get(sortKeyIdx).getName().equalsIgnoreCase(dropColName)) {
                        throw new DdlException("Can not drop sort column in primary data model table");
                    }
                }
            }
        } else if (KeysType.UNIQUE_KEYS == olapTable.getKeysType()) {
            long baseIndexId = olapTable.getBaseIndexId();
            List<Column> baseSchema = indexSchemaMap.get(baseIndexId);
            boolean isKey = baseSchema.stream().anyMatch(c -> c.isKey() && c.getName().equalsIgnoreCase(dropColName));
            lightSchemaChange = !isKey;
            if (isKey) {
                throw new DdlException("Can not drop key column in Unique data model table");
            }
        } else if (KeysType.AGG_KEYS == olapTable.getKeysType()) {
            if (null == targetIndexName) {
                // drop column in base table
                long baseIndexId = olapTable.getBaseIndexId();
                List<Column> baseSchema = indexSchemaMap.get(baseIndexId);
                boolean isKey = baseSchema.stream().anyMatch(c -> c.isKey() && c.getName().equalsIgnoreCase(dropColName));
                lightSchemaChange = !isKey;
                boolean hasReplaceColumn = baseSchema.stream().map(Column::getAggregationType)
                        .anyMatch(agg -> agg == AggregateType.REPLACE || agg == AggregateType.REPLACE_IF_NOT_NULL);
                if (isKey && hasReplaceColumn) {
                    throw new DdlException("Can not drop key column when table has value column with REPLACE aggregation method");
                }
            } else {
                // drop column in rollup and base index
                long targetIndexId = olapTable.getIndexIdByName(targetIndexName);
                List<Column> targetIndexSchema = indexSchemaMap.get(targetIndexId);
                boolean isKey = targetIndexSchema.stream().anyMatch(c -> c.isKey() && c.getName().equalsIgnoreCase(dropColName));
                lightSchemaChange = !isKey;
                boolean hasReplaceColumn = targetIndexSchema.stream().map(Column::getAggregationType)
                        .anyMatch(agg -> agg == AggregateType.REPLACE || agg == AggregateType.REPLACE_IF_NOT_NULL);
                if (isKey && hasReplaceColumn) {
                    throw new DdlException(
                            "Can not drop key column when rollup has value column with REPLACE aggregation method");
                }
            }
        } else if (KeysType.DUP_KEYS == olapTable.getKeysType()) {
            long baseIndexId = olapTable.getBaseIndexId();
            List<Column> baseSchema = indexSchemaMap.get(baseIndexId);
            for (Column column : baseSchema) {
                if (column.isKey() && column.getName().equalsIgnoreCase(dropColName)) {
                    lightSchemaChange = false;
                    break;
                }
            }
        }

        // Remove all Index that contains a column with the name dropColName.
        indexes.removeIf(index -> index.getColumns().stream().anyMatch(c -> c.equalsIgnoreCase(dropColName)));

        if (targetIndexName == null) {
            // if not specify rollup index, column should be dropped from both base and rollup indexes.
            long baseIndexId = olapTable.getBaseIndexId();
            LinkedList<Column> columns = indexSchemaMap.get(baseIndexId);
            Iterator<Column> columnIterator = columns.iterator();
            boolean removed = false;
            while (columnIterator.hasNext()) {
                Column column = columnIterator.next();
                if (column.getName().equalsIgnoreCase(dropColName)) {
                    columnIterator.remove();
                    removed = true;
                    if (column.isKey()) {
                        lightSchemaChange = false;
                    }
                }
            }

            if (!removed) {
                throw new DdlException("Column does not exists: " + dropColName);
            }

            for (Long indexId : olapTable.getIndexIdListExceptBaseIndex()) {
                columns = indexSchemaMap.get(indexId);
                columnIterator = columns.iterator();
                while (columnIterator.hasNext()) {
                    Column column = columnIterator.next();
                    if (column.getName().equalsIgnoreCase(dropColName)) {
                        columnIterator.remove();
                        if (column.isKey()) {
                            lightSchemaChange = false;
                        }
                    }
                }
            }
        } else {
            // if specify rollup index, only drop column from specified rollup index
            long targetIndexId = olapTable.getIndexIdByName(targetIndexName);
            LinkedList<Column> columns = indexSchemaMap.get(targetIndexId);
            Iterator<Column> columnIterator = columns.iterator();
            boolean removed = false;
            while (columnIterator.hasNext()) {
                Column column = columnIterator.next();
                if (column.getName().equalsIgnoreCase(dropColName)) {
                    columnIterator.remove();
                    removed = true;
                    if (column.isKey()) {
                        lightSchemaChange = false;
                    }
                }
            }

            if (!removed) {
                throw new DdlException("Column does not exists: " + dropColName);
            }
        }
        return lightSchemaChange;
    }

    // User can modify column type and column position
    private void processModifyColumn(ModifyColumnClause alterClause, OlapTable olapTable,
            Map<Long, LinkedList<Column>> indexSchemaMap) throws DdlException {
        Column modColumn = alterClause.getColumn();
        if (KeysType.PRIMARY_KEYS == olapTable.getKeysType()) {
            if (olapTable.getBaseColumn(modColumn.getName()).isKey()) {
                throw new DdlException("Can not modify key column: " + modColumn.getName() + " for primary key table");
            }
            MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(olapTable.getBaseIndexId());
            if (indexMeta.getSortKeyIdxes() != null) {
                for (Integer sortKeyIdx : indexMeta.getSortKeyIdxes()) {
                    if (indexMeta.getSchema().get(sortKeyIdx).getName().equalsIgnoreCase(modColumn.getName())) {
                        throw new DdlException("Can not modify sort column in primary data model table");
                    }
                }
            }
            if (modColumn.getAggregationType() != null) {
                throw new DdlException("Can not assign aggregation method on column in Primary data model table: " +
                        modColumn.getName());
            }
            modColumn.setAggregationType(AggregateType.REPLACE, true);
        } else if (KeysType.AGG_KEYS == olapTable.getKeysType()) {
            if (modColumn.isKey() && null != modColumn.getAggregationType()) {
                throw new DdlException("Can not assign aggregation method on key column: " + modColumn.getName());
            } else if (null == modColumn.getAggregationType()) {
                Preconditions.checkArgument(modColumn.getType().isScalarType());
                // in aggregate key table, no aggregation method indicate key column
                modColumn.setIsKey(true);
            }
        } else if (KeysType.UNIQUE_KEYS == olapTable.getKeysType()) {
            if (null != modColumn.getAggregationType()) {
                throw new DdlException("Can not assign aggregation method on column in Unique data model table: " +
                        modColumn.getName());
            }
            if (!modColumn.isKey()) {
                modColumn.setAggregationType(AggregateType.REPLACE, true);
            }
        } else {
            if (null != modColumn.getAggregationType()) {
                throw new DdlException("Can not assign aggregation method on column in Duplicate data model table: " +
                        modColumn.getName());
            }
            if (!modColumn.isKey()) {
                modColumn.setAggregationType(AggregateType.NONE, true);
            }
        }

        if (modColumn.materializedColumnExpr() == null && olapTable.hasMaterializedColumn()) {
            for (Column column : olapTable.getFullSchema()) {
                if (!column.isMaterializedColumn()) {
                    continue;
                }
                List<SlotRef> slots = column.getMaterializedColumnRef();
                for (SlotRef slot : slots) {
                    if (slot.getColumnName().equals(modColumn.getName())) {
                        throw new DdlException("Do not support modify column: " + modColumn.getName() +
                                ", because it associates with the materialized column");
                    }
                }
            }
        }

        ColumnPosition columnPos = alterClause.getColPos();
        String targetIndexName = alterClause.getRollupName();
        checkIndexExists(olapTable, targetIndexName);

        String baseIndexName = olapTable.getName();
        checkAssignedTargetIndexName(baseIndexName, targetIndexName);

        if (targetIndexName != null && columnPos == null) {
            throw new DdlException("Do not need to specify index name when just modifying column type");
        }

        String indexNameForFindingColumn = targetIndexName;
        if (indexNameForFindingColumn == null) {
            indexNameForFindingColumn = baseIndexName;
        }

        long indexIdForFindingColumn = olapTable.getIndexIdByName(indexNameForFindingColumn);

        // find modified column
        List<Column> schemaForFinding = indexSchemaMap.get(indexIdForFindingColumn);
        String newColName = modColumn.getName();
        boolean hasColPos = (columnPos != null && !columnPos.isFirst());
        boolean found = false;
        boolean typeChanged = false;
        int modColIndex = -1;
        int lastColIndex = -1;
        for (int i = 0; i < schemaForFinding.size(); i++) {
            Column col = schemaForFinding.get(i);
            if (col.getName().equalsIgnoreCase(newColName)) {
                modColIndex = i;
                found = true;
                if (!col.equals(modColumn)) {
                    typeChanged = true;
                }
            }
            if (hasColPos) {
                if (col.getName().equalsIgnoreCase(columnPos.getLastCol())) {
                    lastColIndex = i;
                }
            } else {
                // save the last Key position
                if (col.isKey()) {
                    lastColIndex = i;
                }
            }
        }
        // mod col not find
        if (!found) {
            throw new DdlException("Column[" + newColName + "] does not exists");
        }

        // last col not find
        if (hasColPos && lastColIndex == -1) {
            throw new DdlException("Column[" + columnPos.getLastCol() + "] does not exists");
        }

        // check if add to first
        if (columnPos != null && columnPos.isFirst()) {
            lastColIndex = -1;
            hasColPos = true;
        }

        Column oriColumn = schemaForFinding.get(modColIndex);

        if (oriColumn.isAutoIncrement()) {
            throw new DdlException("Can't not modify a AUTO_INCREMENT column");
        }

        // retain old column name
        modColumn.setName(oriColumn.getName());
        modColumn.setUniqueId(oriColumn.getUniqueId());

        if (!oriColumn.isMaterializedColumn() && modColumn.isMaterializedColumn()) {
            throw new DdlException("Can not modify a non-materialized column to a materialized column");
        }

        if (oriColumn.isMaterializedColumn() && !modColumn.isMaterializedColumn()) {
            throw new DdlException("Can not modify a materialized column to a non-materialized column");
        }

        if (oriColumn.isMaterializedColumn() && GlobalStateMgr.getCurrentState().getIdToDb() != null) {
            Database db = null;
            for (Map.Entry<Long, Database> entry : GlobalStateMgr.getCurrentState().getIdToDb().entrySet()) {
                db = entry.getValue();
                if (db.getTable(olapTable.getId()) != null) {
                    break;
                }
            }

            List<Table> tbls = db.getTables();
            for (Table tbl : tbls) {
                Map<Long, MaterializedIndexMeta> metaMap = olapTable.getIndexIdToMeta();

                for (Map.Entry<Long, MaterializedIndexMeta> entry : metaMap.entrySet()) {
                    Long id = entry.getKey();
                    if (id == olapTable.getBaseIndexId()) {
                        continue;
                    }
                    MaterializedIndexMeta meta = entry.getValue();
                    List<Column> schema = meta.getSchema();

                    for (Column rollupCol : schema) {
                        if (rollupCol.getName().equals(oriColumn.getName())) {
                            throw new DdlException("Can not modify a materialized column, because there are MVs ref to it");
                        }
                    }
                }
            }
        }

        // handle the move operation in 'indexForFindingColumn' if has
        if (hasColPos) {
            // move col
            if (lastColIndex > modColIndex) {
                schemaForFinding.add(lastColIndex + 1, modColumn);
                schemaForFinding.remove(modColIndex);
            } else if (lastColIndex < modColIndex) {
                schemaForFinding.remove(modColIndex);
                schemaForFinding.add(lastColIndex + 1, modColumn);
            } else {
                throw new DdlException("Column[" + columnPos.getLastCol() + "] modify position is invalid");
            }
        } else {
            schemaForFinding.set(modColIndex, modColumn);
        }

        // check if column being mod
        if (!modColumn.equals(oriColumn)) {
            // column is mod. we have to mod this column in all indices

            // handle other indices
            // 1 find other indices which contain this column
            List<Long> otherIndexIds = new ArrayList<>();
            for (Map.Entry<Long, List<Column>> entry : olapTable.getIndexIdToSchema().entrySet()) {
                if (entry.getKey() == indexIdForFindingColumn) {
                    // skip the index we used to find column. it has been handled before
                    continue;
                }
                List<Column> schema = entry.getValue();
                for (Column column : schema) {
                    if (column.getName().equalsIgnoreCase(modColumn.getName())) {
                        otherIndexIds.add(entry.getKey());
                        break;
                    }
                }
            }

            if (KeysType.AGG_KEYS == olapTable.getKeysType() || KeysType.UNIQUE_KEYS == olapTable.getKeysType() ||
                    KeysType.PRIMARY_KEYS == olapTable.getKeysType()) {
                for (Long otherIndexId : otherIndexIds) {
                    List<Column> otherIndexSchema = indexSchemaMap.get(otherIndexId);
                    modColIndex = -1;
                    for (int i = 0; i < otherIndexSchema.size(); i++) {
                        if (otherIndexSchema.get(i).getName().equalsIgnoreCase(modColumn.getName())) {
                            modColIndex = i;
                            break;
                        }
                    }
                    Preconditions.checkState(modColIndex != -1);
                    // replace the old column
                    otherIndexSchema.set(modColIndex, modColumn);
                } //  end for other indices
            } else {
                // DUPLICATE data model has a little
                for (Long otherIndexId : otherIndexIds) {
                    List<Column> otherIndexSchema = indexSchemaMap.get(otherIndexId);
                    modColIndex = -1;
                    for (int i = 0; i < otherIndexSchema.size(); i++) {
                        if (otherIndexSchema.get(i).getName().equalsIgnoreCase(modColumn.getName())) {
                            modColIndex = i;
                            break;
                        }
                    }

                    Preconditions.checkState(modColIndex != -1);
                    // replace the old column
                    Column oldCol = otherIndexSchema.get(modColIndex);
                    Column otherCol = new Column(modColumn);
                    otherCol.setIsKey(oldCol.isKey());
                    if (null != oldCol.getAggregationType()) {
                        otherCol.setAggregationType(oldCol.getAggregationType(), oldCol.isAggregationTypeImplicit());
                    } else {
                        otherCol.setAggregationType(null, oldCol.isAggregationTypeImplicit());
                    }
                    otherIndexSchema.set(modColIndex, otherCol);
                }
            }
        } // end for handling other indices

        if (typeChanged) {
            /*
             * In new alter table process (AlterJobV2), any modified columns are treated as new columns.
             * But the modified columns' name does not changed. So in order to distinguish this, we will add
             * a prefix in the name of these modified columns.
             * This prefix only exist during the schema change process. Once the schema change is finished,
             * it will be removed.
             *
             * After adding this prefix, modify a column is just same as 'add' a column.
             *
             * And if the column type is not changed, the same column name is still to the same column type,
             * so no need to add prefix.
             */
            modColumn.setName(SHADOW_NAME_PRFIX + modColumn.getName());
        }

    }

    private void processReorderColumn(ReorderColumnsClause alterClause, OlapTable olapTable,
            Map<Long, LinkedList<Column>> indexSchemaMap) throws DdlException {
        List<String> orderedColNames = alterClause.getColumnsByPos();
        String targetIndexName = alterClause.getRollupName();
        checkIndexExists(olapTable, targetIndexName);

        String baseIndexName = olapTable.getName();
        checkAssignedTargetIndexName(baseIndexName, targetIndexName);

        if (targetIndexName == null) {
            targetIndexName = baseIndexName;
        }

        long targetIndexId = olapTable.getIndexIdByName(targetIndexName);

        LinkedList<Column> newSchema = new LinkedList<>();
        LinkedList<Column> targetIndexSchema = indexSchemaMap.get(targetIndexId);

        // check and create new ordered column list
        Set<String> colNameSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String colName : orderedColNames) {
            Optional<Column> oneCol = targetIndexSchema.stream().filter(c -> c.getName().equalsIgnoreCase(colName)).findFirst();
            if (!oneCol.isPresent()) {
                throw new DdlException("Column[" + colName + "] not exists");
            }
            if (!colNameSet.add(colName)) {
                throw new DdlException("Duplicated column[" + colName + "]");
            }
            newSchema.add(oneCol.get());
        }
        if (newSchema.size() != targetIndexSchema.size()) {
            throw new DdlException("Reorder stmt should contains all columns");
        }
        // replace the old column list
        indexSchemaMap.put(targetIndexId, newSchema);
    }

    private void processReorderColumnOfPrimaryKey(ReorderColumnsClause alterClause, OlapTable olapTable,
            Map<Long, LinkedList<Column>> indexSchemaMap, List<Integer> sortKeyIdxes)
            throws DdlException {
        LinkedList<Column> targetIndexSchema = indexSchemaMap.get(olapTable.getIndexIdByName(olapTable.getName()));
        // check sort key column list
        Set<String> colNameSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

        for (String colName : alterClause.getColumnsByPos()) {
            Optional<Column> oneCol = targetIndexSchema.stream().filter(c -> c.getName().equalsIgnoreCase(colName)).findFirst();
            if (!oneCol.isPresent()) {
                throw new DdlException("Column[" + colName + "] not exists");
            }
            if (!colNameSet.add(colName)) {
                throw new DdlException("Duplicated column[" + colName + "]");
            }
            int sortKeyIdx = targetIndexSchema.indexOf(oneCol.get());
            sortKeyIdxes.add(sortKeyIdx);
            Type t = oneCol.get().getType();
            if (!(t.isBoolean() || t.isIntegerType() || t.isLargeint() || t.isVarchar() || t.isDate() ||
                    t.isDatetime())) {
                throw new DdlException("Sort key column[" + colName + "] type not supported: " + t);
            }
        }
    }

    /**
     * @param olapTable
     * @param newColumn Add 'newColumn' to specified index.
     * @param columnPos
     * @param targetIndexId
     * @param baseIndexId
     * @param indexSchemaMap Modified schema will be saved in 'indexSchemaMap'
     * @param newColNameSet
     * @return true: can light schema change, false: cannot
     * @throws DdlException
     */
    private boolean addColumnInternal(OlapTable olapTable, Column newColumn, ColumnPosition columnPos,
            long targetIndexId, long baseIndexId,
            Map<Long, LinkedList<Column>> indexSchemaMap,
            Set<String> newColNameSet) throws DdlException {

        Column.DefaultValueType defaultValueType = newColumn.getDefaultValueType();
        if (defaultValueType == Column.DefaultValueType.VARY) {
            throw new DdlException("unsupported default expr:" + newColumn.getDefaultExpr().getExpr());
        }

        //only new table generate ColUniqueId, exist table do not.
        boolean lightSchemaChange = olapTable.getMaxColUniqueId() > Column.COLUMN_UNIQUE_ID_INIT_VALUE;

        String newColName = newColumn.getName();
        //make sure olapTable has locked
        LOG.debug("table: {}, newColumn: {}, uniqueId: {}", olapTable.getName(), newColumn.getName(),
                newColumn.getUniqueId());

        // check the validation of aggregation method on column.
        // also fill the default aggregation method if not specified.
        if (KeysType.PRIMARY_KEYS == olapTable.getKeysType()) {
            if (newColumn.isKey()) {
                throw new DdlException("Can not add key column: " + newColName + " for primary key table");
            }
            if (newColumn.getAggregationType() != null) {
                throw new DdlException(
                        "Can not assign aggregation method on column in Primary data model table: " + newColName);
            }
            newColumn.setAggregationType(AggregateType.REPLACE, true);
        } else if (KeysType.AGG_KEYS == olapTable.getKeysType()) {
            if (newColumn.isKey() && newColumn.getAggregationType() != null) {
                throw new DdlException("Can not assign aggregation method on key column: " + newColName);
            } else if (null == newColumn.getAggregationType()) {
                Type type = newColumn.getType();
                if (!type.canDistributedBy()) {
                    throw new DdlException(
                            "column without agg function will be treated as key column for aggregate table, " + type +
                                    " type can not be key column");
                }
                newColumn.setIsKey(true);
            } else if (newColumn.getAggregationType() == AggregateType.SUM
                    && newColumn.getDefaultValue() != null && !"0".equals(newColumn.getDefaultValue())) {
                throw new DdlException(
                        "The default value of '" + newColName + "' with SUM aggregation function must be zero");
            }
        } else if (KeysType.UNIQUE_KEYS == olapTable.getKeysType()) {
            if (newColumn.getAggregationType() != null) {
                throw new DdlException(
                        "Can not assign aggregation method on column in Unique data model table: " + newColName);
            }
            if (!newColumn.isKey()) {
                newColumn.setAggregationType(AggregateType.REPLACE, true);
            }
        } else {
            if (newColumn.getAggregationType() != null) {
                throw new DdlException(
                        "Can not assign aggregation method on column in Duplicate data model table: " + newColName);
            }
            if (!newColumn.isKey()) {
                if (targetIndexId != -1L && olapTable.getIndexMetaByIndexId(targetIndexId).getKeysType() == KeysType.AGG_KEYS) {
                    throw new DdlException("Please add non-key column on base table directly");
                }
                newColumn.setAggregationType(AggregateType.NONE, true);
            }
        }

        // hll must be used in agg_keys
        if (newColumn.getType().isHllType() && KeysType.AGG_KEYS != olapTable.getKeysType()) {
            throw new DdlException("HLL type column can only be in Aggregation data model table: " + newColName);
        }

        if (newColumn.getAggregationType() == AggregateType.BITMAP_UNION && KeysType.AGG_KEYS != olapTable.getKeysType()) {
            throw new DdlException("BITMAP_UNION must be used in AGG_KEYS");
        }

        if (newColumn.getAggregationType() == AggregateType.PERCENTILE_UNION && KeysType.AGG_KEYS != olapTable.getKeysType()) {
            throw new DdlException("PERCENTILE_UNION must be used in AGG_KEYS");
        }

        //type key column do not allow light schema change.
        if (newColumn.isKey()) {
            lightSchemaChange = false;
        }

        // check if the new column already exist in base schema.
        // do not support adding new column which already exist in base schema.
        Optional<Column> foundColumn = olapTable.getBaseSchema().stream()
                .filter(c -> c.getName().equalsIgnoreCase(newColName)).findFirst();
        if (foundColumn.isPresent() && newColumn.equals(foundColumn.get())) {
            throw new DdlException(
                    "Can not add column which already exists in base table: " + newColName);
        }

        /*
         * add new column to indexes.
         * PRIMARY:
         *      1. add the new column to base index
         * UNIQUE:
         *      1. If new column is key, it should be added to all indexes.
         *      2. Else, add the new column to base index and specified rollup index.
         * DUPLICATE:
         *      1. If not specify rollup index, just add it to base index.
         *      2. Else, first add it to specify rollup index. Then if the new column is key, add it to base
         *          index, at the end of all other existing key columns. If new new column is value, add it to
         *          base index by user specified position.
         * AGGREGATION:
         *      1. Add it to base index, as well as specified rollup index.
         */
        if (KeysType.PRIMARY_KEYS == olapTable.getKeysType()) {
            List<Column> modIndexSchema = indexSchemaMap.get(baseIndexId);
            checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, true);
            lightSchemaChange = false;
            if (targetIndexId != -1L) {
                throw new DdlException("Can not add column: " + newColName + " to rollup index");
            }
        } else if (KeysType.UNIQUE_KEYS == olapTable.getKeysType()) {
            List<Column> modIndexSchema;
            if (newColumn.isKey()) {
                // add key column to unique key table
                // add to all indexes including base and rollup
                for (Map.Entry<Long, LinkedList<Column>> entry : indexSchemaMap.entrySet()) {
                    modIndexSchema = entry.getValue();
                    boolean isBaseIdex = entry.getKey() == baseIndexId;
                    checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, isBaseIdex);
                }
            } else {
                // 1. add to base table
                modIndexSchema = indexSchemaMap.get(baseIndexId);
                checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, true);
                if (targetIndexId == -1L) {
                    return lightSchemaChange;
                }
                // 2. add to rollup
                lightSchemaChange = false;
                modIndexSchema = indexSchemaMap.get(targetIndexId);
                checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, false);
            }
        } else if (KeysType.DUP_KEYS == olapTable.getKeysType()) {
            if (targetIndexId == -1L) {
                // add to base index
                List<Column> modIndexSchema = indexSchemaMap.get(baseIndexId);
                checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, true);
                // no specified target index. return
                return lightSchemaChange;
            } else {
                // add to rollup index
                lightSchemaChange = false;
                List<Column> modIndexSchema = indexSchemaMap.get(targetIndexId);
                checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, false);

                if (newColumn.isKey()) {
                    /*
                     * if add column in rollup is key,
                     * then put the column in base table as the last key column
                     */
                    modIndexSchema = indexSchemaMap.get(baseIndexId);
                    checkAndAddColumn(modIndexSchema, newColumn, null, newColNameSet, true);
                } else {
                    modIndexSchema = indexSchemaMap.get(baseIndexId);
                    checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, true);
                }
            }
        } else {
            // check if has default value. this should be done in Analyze phase
            // 1. add to base index first
            List<Column> modIndexSchema = indexSchemaMap.get(baseIndexId);
            checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, true);

            if (targetIndexId == -1L) {
                // no specified target index. return
                return lightSchemaChange;
            }

            lightSchemaChange = false;
            // 2. add to rollup index
            modIndexSchema = indexSchemaMap.get(targetIndexId);
            checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, false);
        }
        return lightSchemaChange;
    }

    /*
     * add new column to specified index schema('modIndexSchema').
     * if 'isBaseIndex' is true, which means 'modIndexSchema' is base index's schema.
     * so we will not check repeat adding of column.
     * For example, user want to add column k1 to both rollup1 and rollup2 in one alter stmt:
     *      ADD COLUMN k1 int to rollup1,
     *      ADD COLUMN k1 int to rollup2
     * So that k1 will be added to base index 'twice', and we just ignore this repeat adding.
     */
    private void checkAndAddColumn(List<Column> modIndexSchema, Column newColumn, ColumnPosition columnPos,
            Set<String> newColNameSet, boolean isBaseIndex) throws DdlException {
        int posIndex = -1;
        String newColName = newColumn.getName();
        boolean hasPos = (columnPos != null && !columnPos.isFirst());
        for (int i = 0; i < modIndexSchema.size(); i++) {
            Column col = modIndexSchema.get(i);
            if (col.getName().equalsIgnoreCase(newColName)) {
                if (!isBaseIndex || !newColNameSet.contains(newColName)) {
                    // if this is not a base index, we should check if user repeatedly add columns
                    throw new DdlException("Repeatedly add column: " + newColName);
                }
                // this is a base index, and the column we check here is added by previous 'add column clause'
                // in same ALTER stmt.
                // so here we will check if the 2 columns is exactly same. if not, throw exception
                if (!col.equals(newColumn)) {
                    throw new DdlException("Repeatedly add same column with different definition: " + newColName);
                }

                // column already exist, return
                return;
            }

            if (hasPos) {
                // after the field
                if (col.getName().equalsIgnoreCase(columnPos.getLastCol())) {
                    posIndex = i;
                }
            } else {
                // save the last Key position
                if (col.isKey()) {
                    posIndex = i;
                }
            }
        }

        // check if lastCol was found
        if (hasPos && posIndex == -1) {
            throw new DdlException("Column[" + columnPos.getLastCol() + "] does not found");
        }

        // check if add to first
        if (columnPos != null && columnPos.isFirst()) {
            posIndex = -1;
            hasPos = true;
        }

        if (hasPos) {
            modIndexSchema.add(posIndex + 1, newColumn);
        } else {
            if (newColumn.isKey()) {
                // key
                modIndexSchema.add(posIndex + 1, newColumn);
            } else {
                // value
                modIndexSchema.add(newColumn);
            }
        }
    }

    private void checkIndexExists(OlapTable olapTable, String targetIndexName) throws DdlException {
        if (targetIndexName != null && !olapTable.hasMaterializedIndex(targetIndexName)) {
            throw new DdlException("Index[" + targetIndexName + "] does not exist in table[" + olapTable.getName()
                    + "]");
        }
    }

    private void checkAssignedTargetIndexName(String baseIndexName, String targetIndexName) throws DdlException {
        // user cannot assign base index to do schema change
        if (targetIndexName != null) {
            if (targetIndexName.equals(baseIndexName)) {
                throw new DdlException("Do not need to assign base index[" + baseIndexName + "] to do schema change");
            }
        }
    }

    private AlterJobV2 createJob(long dbId, OlapTable olapTable, Map<Long, LinkedList<Column>> indexSchemaMap,
            Map<String, String> propertyMap, List<Index> indexes) throws UserException {
        if (olapTable.getState() == OlapTableState.ROLLUP) {
            throw new DdlException("Table[" + olapTable.getName() + "]'s is doing ROLLUP job");
        }

        // for now table's state can only be NORMAL
        Preconditions.checkState(olapTable.getState() == OlapTableState.NORMAL, olapTable.getState().name());

        // process properties first
        // for now. properties has 3 options
        // property 1. to specify short key column count.
        // eg.
        //     "indexname1#short_key" = "3"
        //     "indexname2#short_key" = "4"
        Map<Long, Map<String, String>> indexIdToProperties = new HashMap<Long, Map<String, String>>();
        if (propertyMap.size() > 0) {
            for (String key : propertyMap.keySet()) {
                if (key.endsWith(PropertyAnalyzer.PROPERTIES_SHORT_KEY)) {
                    // short key
                    String[] keyArray = key.split("#");
                    if (keyArray.length != 2 || keyArray[0].isEmpty()
                            || !keyArray[1].equals(PropertyAnalyzer.PROPERTIES_SHORT_KEY)) {
                        throw new DdlException("Invalid alter table property: " + key);
                    }

                    HashMap<String, String> prop = new HashMap<String, String>();

                    if (!olapTable.hasMaterializedIndex(keyArray[0])) {
                        throw new DdlException("Index[" + keyArray[0] + "] does not exist");
                    }

                    prop.put(PropertyAnalyzer.PROPERTIES_SHORT_KEY, propertyMap.get(key));
                    indexIdToProperties.put(olapTable.getIndexIdByName(keyArray[0]), prop);
                }
            } // end for property keys
        }

        // for bitmapIndex
        boolean hasIndexChange = false;
        Set<Index> newSet = new HashSet<>(indexes);
        Set<Index> oriSet = new HashSet<>(olapTable.getIndexes());
        if (!newSet.equals(oriSet)) {
            hasIndexChange = true;
        }

        // property 2. bloom filter
        // eg. "bloom_filter_columns" = "k1,k2", "bloom_filter_fpp" = "0.05"
        Set<String> bfColumns = null;
        double bfFpp = 0;
        try {
            bfColumns = PropertyAnalyzer
                    .analyzeBloomFilterColumns(propertyMap, indexSchemaMap.get(olapTable.getBaseIndexId()),
                            olapTable.getKeysType() == KeysType.PRIMARY_KEYS);
            bfFpp = PropertyAnalyzer.analyzeBloomFilterFpp(propertyMap);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // check bloom filter has change
        boolean hasBfChange = false;
        Set<String> oriBfColumns = olapTable.getCopiedBfColumns();
        double oriBfFpp = olapTable.getBfFpp();
        if (bfColumns != null) {
            if (bfFpp == 0) {
                // columns: yes, fpp: no
                if (bfColumns.equals(oriBfColumns)) {
                    throw new DdlException("Bloom filter index has no change");
                }

                if (oriBfColumns == null) {
                    bfFpp = FeConstants.DEFAULT_BLOOM_FILTER_FPP;
                } else {
                    bfFpp = oriBfFpp;
                }
            } else {
                // columns: yes, fpp: yes
                if (bfColumns.equals(oriBfColumns) && bfFpp == oriBfFpp) {
                    throw new DdlException("Bloom filter index has no change");
                }
            }

            hasBfChange = true;
        } else {
            if (bfFpp == 0) {
                // columns: no, fpp: no
                bfFpp = oriBfFpp;
            } else {
                // columns: no, fpp: yes
                if (bfFpp == oriBfFpp) {
                    throw new DdlException("Bloom filter index has no change");
                }
                if (oriBfColumns == null) {
                    throw new DdlException("Bloom filter index has no change");
                }

                hasBfChange = true;
            }

            bfColumns = oriBfColumns;
        }

        if (bfColumns != null && bfColumns.isEmpty()) {
            bfColumns = null;
        }
        if (bfColumns == null) {
            bfFpp = 0;
        }

        // property 3: timeout
        long timeoutSecond = PropertyAnalyzer.analyzeTimeout(propertyMap, Config.alter_table_timeout_second);

        // create job
        AlterJobV2Builder jobBuilder = olapTable.alterTable();
        jobBuilder.withJobId(GlobalStateMgr.getCurrentState().getNextId())
                .withDbId(dbId)
                .withTimeoutSeconds(timeoutSecond)
                .withAlterIndexInfo(hasIndexChange, indexes)
                .withStartTime(ConnectContext.get().getStartTime())
                .withBloomFilterColumns(bfColumns, bfFpp)
                .withBloomFilterColumnsChanged(hasBfChange);

        // begin checking each table
        // ATTN: DO NOT change any meta in this loop
        long tableId = olapTable.getId();
        for (Long alterIndexId : indexSchemaMap.keySet()) {
            List<Column> originSchema = olapTable.getSchemaByIndexId(alterIndexId);
            List<Column> alterSchema = indexSchemaMap.get(alterIndexId);

            // 0. check if unchanged
            boolean hasColumnChange = !originSchema.equals(alterSchema);

            // if has column changed, alter it.
            // else:
            //     if no bf change, no alter
            //     if has bf change, should check
            boolean needAlter = false;
            if (hasColumnChange) {
                needAlter = true;
            } else if (hasBfChange) {
                for (Column alterColumn : alterSchema) {
                    String columnName = alterColumn.getName();

                    boolean isOldBfColumn = oriBfColumns != null && oriBfColumns.contains(columnName);

                    boolean isNewBfColumn = bfColumns != null && bfColumns.contains(columnName);

                    if (isOldBfColumn != isNewBfColumn) {
                        // bf column change
                        needAlter = true;
                    } else if (isOldBfColumn && isNewBfColumn && oriBfFpp != bfFpp) {
                        // bf fpp change
                        needAlter = true;
                    }

                    if (needAlter) {
                        break;
                    }
                }
            } else if (hasIndexChange) {
                needAlter = true;
            }

            if (!needAlter) {
                LOG.debug("index[{}] is not changed. ignore", alterIndexId);
                continue;
            }

            LOG.debug("index[{}] is changed. start checking...", alterIndexId);
            // 1. check order: a) has key; b) value after key
            boolean meetValue = false;
            boolean hasKey = false;
            for (Column column : alterSchema) {
                if (column.isKey() && meetValue) {
                    throw new DdlException("Invalid column order. value should be after key. index["
                            + olapTable.getIndexNameById(alterIndexId) + "]");
                }
                if (!column.isKey()) {
                    meetValue = true;
                } else {
                    hasKey = true;
                }
            }
            if (!hasKey) {
                throw new DdlException("No key column left. index[" + olapTable.getIndexNameById(alterIndexId) + "]");
            }

            // 2. check compatible
            for (Column alterColumn : alterSchema) {
                Optional<Column> col = originSchema.stream().filter(c -> c.nameEquals(alterColumn.getName(), true)).findFirst();
                if (col.isPresent() && !alterColumn.equals(col.get())) {
                    col.get().checkSchemaChangeAllowed(alterColumn);
                }
            }

            // 3. check partition key
            if (hasColumnChange && olapTable.getPartitionInfo().isRangePartition()) {
                List<Column> partitionColumns = ((RangePartitionInfo) olapTable.getPartitionInfo()).getPartitionColumns();
                for (Column partitionCol : partitionColumns) {
                    String colName = partitionCol.getName();
                    Optional<Column> col = alterSchema.stream().filter(c -> c.nameEquals(colName, true)).findFirst();
                    if (col.isPresent() && !col.get().equals(partitionCol)) {
                        throw new DdlException("Can not modify partition column[" + colName + "]. index["
                                + olapTable.getIndexNameById(alterIndexId) + "]");
                    }
                    if (!col.isPresent() && alterIndexId == olapTable.getBaseIndexId()) {
                        // 2.1 partition column cannot be deleted.
                        throw new DdlException("Partition column[" + partitionCol.getName()
                                + "] cannot be dropped. index[" + olapTable.getIndexNameById(alterIndexId) + "]");
                        // ATTN. partition columns' order also need remaining unchanged.
                        // for now, we only allow one partition column, so no need to check order.
                    }
                } // end for partitionColumns
            }

            // 4. check distribution key:
            if (hasColumnChange && olapTable.getDefaultDistributionInfo().getType() == DistributionInfoType.HASH) {
                List<Column> distributionColumns =
                        ((HashDistributionInfo) olapTable.getDefaultDistributionInfo()).getDistributionColumns();
                for (Column distributionCol : distributionColumns) {
                    String colName = distributionCol.getName();
                    Optional<Column> col = alterSchema.stream().filter(c -> c.nameEquals(colName, true)).findFirst();
                    if (col.isPresent() && !col.get().equals(distributionCol)) {
                        throw new DdlException("Can not modify distribution column[" + colName + "]. index["
                                + olapTable.getIndexNameById(alterIndexId) + "]");
                    }
                    if (!col.isPresent() && alterIndexId == olapTable.getBaseIndexId()) {
                        // 2.2 distribution column cannot be deleted.
                        throw new DdlException("Distribution column[" + distributionCol.getName()
                                + "] cannot be dropped. index[" + olapTable.getIndexNameById(alterIndexId) + "]");
                    }
                } // end for distributionCols
            }

            // 5. calc short key
            List<Integer> sortKeyIdxes = new ArrayList<>();
            if (KeysType.PRIMARY_KEYS == olapTable.getKeysType()) {
                MaterializedIndexMeta index = olapTable.getIndexMetaByIndexId(alterIndexId);
                if (index.getSortKeyIdxes() != null) {
                    List<Integer> originSortKeyIdxes = index.getSortKeyIdxes();
                    for (Integer colIdx : originSortKeyIdxes) {
                        String columnName = index.getSchema().get(colIdx).getName();
                        Optional<Column> oneCol =
                                alterSchema.stream().filter(c -> c.getName().equalsIgnoreCase(columnName)).findFirst();
                        if (!oneCol.isPresent()) {
                            LOG.warn("Sort Key Column[" + columnName + "] not exists in new schema");
                            throw new DdlException("Sort Key Column[" + columnName + "] not exists in new schema");
                        }
                        int sortKeyIdx = alterSchema.indexOf(oneCol.get());
                        sortKeyIdxes.add(sortKeyIdx);
                    }
                }
            }
            if (!sortKeyIdxes.isEmpty()) {
                short newShortKeyCount = GlobalStateMgr.calcShortKeyColumnCount(alterSchema,
                        indexIdToProperties.get(alterIndexId),
                        sortKeyIdxes);
                LOG.debug("alter index[{}] short key column count: {}", alterIndexId, newShortKeyCount);
                jobBuilder.withNewIndexShortKeyCount(alterIndexId,
                        newShortKeyCount).withNewIndexSchema(alterIndexId, alterSchema);
                jobBuilder.withSortKeyIdxes(sortKeyIdxes);
                LOG.debug("schema change[{}-{}-{}] check pass.", dbId, tableId, alterIndexId);
            } else {
                short newShortKeyCount = GlobalStateMgr.calcShortKeyColumnCount(alterSchema,
                        indexIdToProperties.get(alterIndexId));
                LOG.debug("alter index[{}] short key column count: {}", alterIndexId, newShortKeyCount);
                jobBuilder.withNewIndexShortKeyCount(alterIndexId,
                        newShortKeyCount).withNewIndexSchema(alterIndexId, alterSchema);
                LOG.debug("schema change[{}-{}-{}] check pass.", dbId, tableId, alterIndexId);
            }
        } // end for indices

        return jobBuilder.build();
    }

    private AlterJobV2 createJobForProcessReorderColumnOfPrimaryKey(long dbId, OlapTable olapTable,
            Map<Long, LinkedList<Column>> indexSchemaMap,
            List<Integer> sortKeyIdxes) throws UserException {
        if (olapTable.getState() == OlapTableState.ROLLUP) {
            throw new DdlException("Table[" + olapTable.getName() + "]'s is doing ROLLUP job");
        }

        // for now table's state can only be NORMAL
        Preconditions.checkState(olapTable.getState() == OlapTableState.NORMAL, olapTable.getState().name());

        // create job
        AlterJobV2Builder jobBuilder = olapTable.alterTable();
        jobBuilder.withJobId(GlobalStateMgr.getCurrentState().getNextId())
                .withDbId(dbId)
                .withTimeoutSeconds(Config.alter_table_timeout_second)
                .withStartTime(ConnectContext.get().getStartTime())
                .withSortKeyIdxes(sortKeyIdxes);

        long tableId = olapTable.getId();
        for (Long alterIndexId : indexSchemaMap.keySet()) {
            List<Column> originSchema = olapTable.getSchemaByIndexId(alterIndexId);

            short newShortKeyCount = 0;
            if (sortKeyIdxes != null) {
                newShortKeyCount = GlobalStateMgr.calcShortKeyColumnCount(originSchema, null, sortKeyIdxes);
            } else {
                newShortKeyCount = GlobalStateMgr.calcShortKeyColumnCount(originSchema, null);
            }

            LOG.debug("alter index[{}] short key column count: {}", alterIndexId, newShortKeyCount);
            jobBuilder.withNewIndexShortKeyCount(alterIndexId, newShortKeyCount).withNewIndexSchema(alterIndexId, originSchema);

            LOG.debug("schema change[{}-{}-{}] check pass.", dbId, tableId, alterIndexId);
        } // end for indices

        return jobBuilder.build();
    }

    @Override
    protected void runAfterCatalogReady() {
        super.runAfterCatalogReady();
        runAlterJobV2();
    }

    private void runAlterJobV2() {
        for (AlterJobV2 alterJob : alterJobsV2.values()) {
            if (alterJob.jobState.isFinalState()) {
                continue;
            }
            alterJob.run();
        }
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        List<List<Comparable>> schemaChangeJobInfos = new LinkedList<>();
        getAlterJobV2Infos(db, schemaChangeJobInfos);

        // sort by "JobId", "PartitionName", "CreateTime", "FinishTime", "IndexName", "IndexState"
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1, 2, 3, 4, 5);
        schemaChangeJobInfos.sort(comparator);
        return schemaChangeJobInfos;
    }

    private void getAlterJobV2Infos(Database db, List<AlterJobV2> alterJobsV2,
            List<List<Comparable>> schemaChangeJobInfos) {
        ConnectContext ctx = ConnectContext.get();
        for (AlterJobV2 alterJob : alterJobsV2) {
            if (alterJob.getDbId() != db.getId()) {
                continue;
            }
            alterJob.getInfo(schemaChangeJobInfos);
        }
    }

    private void getAlterJobV2Infos(Database db, List<List<Comparable>> schemaChangeJobInfos) {
        getAlterJobV2Infos(db, ImmutableList.copyOf(alterJobsV2.values()), schemaChangeJobInfos);
    }

    public Optional<Long> getActiveTxnIdOfTable(long tableId) {
        Map<Long, AlterJobV2> alterJobV2Map = getAlterJobsV2();
        for (AlterJobV2 job : alterJobV2Map.values()) {
            AlterJobV2.JobState state = job.getJobState();
            if (job.getTableId() == tableId && state != AlterJobV2.JobState.FINISHED && state != AlterJobV2.JobState.CANCELLED) {
                return job.getTransactionId();
            }
        }
        return Optional.empty();
    }

    @VisibleForTesting
    @Nullable
    public AlterJobV2 analyzeAndCreateJob(List<AlterClause> alterClauses, Database db, OlapTable olapTable) throws UserException {

        //alterClauses can or cannot light schema change
        boolean lightSchemaChange = true;
        //for multi add colmuns clauses
        IntSupplier colUniqueIdSupplier = new IntSupplier() {
            private int pendingMaxColUniqueId = olapTable.getMaxColUniqueId();

            @Override
            public int getAsInt() {
                pendingMaxColUniqueId++;
                return pendingMaxColUniqueId;
            }
        };
        // index id -> index schema
        Map<Long, LinkedList<Column>> indexSchemaMap = new HashMap<>();
        for (Map.Entry<Long, List<Column>> entry : olapTable.getIndexIdToSchema().entrySet()) {
            indexSchemaMap.put(entry.getKey(), new LinkedList<>(entry.getValue()));
        }
        List<Index> newIndexes = olapTable.getCopiedIndexes();
        Map<String, String> propertyMap = new HashMap<>();
        for (AlterClause alterClause : alterClauses) {
            Map<String, String> properties = alterClause.getProperties();
            if (properties != null) {
                if (propertyMap.isEmpty()) {
                    propertyMap.putAll(properties);
                } else {
                    throw new DdlException("reduplicated PROPERTIES");
                }

                // modification of colocate property is handle alone.
                // And because there should be only one colocate property modification clause in stmt,
                // so just return after finished handling.
                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)) {
                    String colocateGroup = properties.get(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH);
                    GlobalStateMgr.getCurrentState().modifyTableColocate(db, olapTable, colocateGroup, false, null);
                    return null;
                } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE)) {
                    GlobalStateMgr.getCurrentState().convertDistributionType(db, olapTable);
                    return null;
                } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK)) {
                    /*
                     * This is only for fixing bug when upgrading StarRocks from 0.9.x to 0.10.x.
                     */
                    sendClearAlterTask(db, olapTable);
                    return null;
                } else if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(properties)) {
                    if (!olapTable.dynamicPartitionExists()) {
                        DynamicPartitionUtil.checkInputDynamicPartitionProperties(properties, olapTable.getPartitionInfo());
                    }
                    if (properties.containsKey(DynamicPartitionProperty.BUCKETS)) {
                        String colocateGroup = olapTable.getColocateGroup();
                        if (colocateGroup != null) {
                            throw new DdlException("The table has a colocate group:" + colocateGroup + ". so cannot " +
                                    "modify dynamic_partition.buckets. Colocate tables must have same bucket number.");
                        }
                    }
                    GlobalStateMgr.getCurrentState().modifyTableDynamicPartition(db, olapTable, properties);
                    return null;
                } else if (properties.containsKey("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
                    Preconditions.checkNotNull(properties.get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
                    GlobalStateMgr.getCurrentState().modifyTableDefaultReplicationNum(db, olapTable, properties);
                    return null;
                } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
                    GlobalStateMgr.getCurrentState().modifyTableReplicationNum(db, olapTable, properties);
                    return null;
                } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM)) {
                    return null;
                } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE)) {
                    return null;
                } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER)) {
                    GlobalStateMgr.getCurrentState().alterTableProperties(db, olapTable, properties);
                    return null;
                } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
                    GlobalStateMgr.getCurrentState().alterTableProperties(db, olapTable, properties);
                    return null;
                }
            }

            if (GlobalStateMgr.getCurrentState().getInsertOverwriteJobMgr().hasRunningOverwriteJob(olapTable.getId())) {
                // because insert overwrite will create tmp partitions
                throw new DdlException("Table[" + olapTable.getName() + "] is doing insert overwrite job, " +
                        "please start schema change after insert overwrite finished");
            }
            // the following operations can not be done when there are temp partitions exist.
            if (olapTable.existTempPartitions()) {
                throw new DdlException("Can not alter table when there are temp partitions in table");
            }

            if (alterClause instanceof AddColumnClause) {
                // add column
                lightSchemaChange =
                        processAddColumn((AddColumnClause) alterClause, olapTable, indexSchemaMap,
                                colUniqueIdSupplier);
            } else if (alterClause instanceof AddColumnsClause) {
                // add columns
                lightSchemaChange =
                        processAddColumns((AddColumnsClause) alterClause, olapTable, indexSchemaMap, colUniqueIdSupplier);
            } else if (alterClause instanceof DropColumnClause) {
                // drop column and drop indexes on this column
                lightSchemaChange =
                        processDropColumn((DropColumnClause) alterClause, olapTable, indexSchemaMap,
                                newIndexes);
            } else if (alterClause instanceof ModifyColumnClause) {
                // modify column
                processModifyColumn((ModifyColumnClause) alterClause, olapTable, indexSchemaMap);
                lightSchemaChange = false;
            } else if (alterClause instanceof ReorderColumnsClause) {
                // reorder column
                lightSchemaChange = false;
                if (olapTable.getKeysType() == KeysType.PRIMARY_KEYS) {
                    List<Integer> sortKeyIdxes = new ArrayList<>();
                    processReorderColumnOfPrimaryKey((ReorderColumnsClause) alterClause, olapTable, indexSchemaMap, sortKeyIdxes);
                    return createJobForProcessReorderColumnOfPrimaryKey(db.getId(), olapTable,
                            indexSchemaMap, sortKeyIdxes);
                } else {
                    processReorderColumn((ReorderColumnsClause) alterClause, olapTable, indexSchemaMap);
                }
            } else if (alterClause instanceof ModifyTablePropertiesClause) {
                // modify table properties
                // do nothing, properties are already in propertyMap
                lightSchemaChange = false;
            } else if (alterClause instanceof CreateIndexClause) {
                lightSchemaChange = false;
                processAddIndex((CreateIndexClause) alterClause, olapTable, newIndexes);
            } else if (alterClause instanceof DropIndexClause) {
                lightSchemaChange = false;
                processDropIndex((DropIndexClause) alterClause, olapTable, newIndexes);
            } else {
                Preconditions.checkState(false);
            }
        } // end for alter clauses

        LOG.debug("processAddColumns, table: {}({}), ligthSchemaChange: {}", olapTable.getName(), olapTable.getId(),
                lightSchemaChange);

        if (lightSchemaChange) {
            long jobId = GlobalStateMgr.getCurrentState().getNextId();
            //for schema change add/drop value column optimize, direct modify table meta.
            modifyTableAddOrDropColumns(db, olapTable, indexSchemaMap, newIndexes, jobId, false);
            return null;
        } else {
            return createJob(db.getId(), olapTable, indexSchemaMap, propertyMap, newIndexes);
        }
    }

    @Override
    public ShowResultSet process(List<AlterClause> alterClauses, Database db, OlapTable olapTable)
            throws UserException {
        AlterJobV2 schemaChangeJob = analyzeAndCreateJob(alterClauses, db, olapTable);
        if (schemaChangeJob == null) {
            return null;
        }

        // set table state
        olapTable.setState(OlapTableState.SCHEMA_CHANGE);

        // 2. add schemaChangeJob
        addAlterJobV2(schemaChangeJob);

        // 3. write edit log
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(schemaChangeJob);
        LOG.info("finished to create schema change job: {}", schemaChangeJob.getJobId());
        return null;
    }

    private void sendClearAlterTask(Database db, OlapTable olapTable) {
        AgentBatchTask batchTask = new AgentBatchTask();
        db.readLock();
        try {
            for (Partition partition : olapTable.getPartitions()) {
                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        int schemaHash = olapTable.getSchemaHashByIndexId(index.getId());
                        for (Tablet tablet : index.getTablets()) {
                            for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                ClearAlterTask alterTask = new ClearAlterTask(replica.getBackendId(), db.getId(),
                                        olapTable.getId(), physicalPartition.getId(), index.getId(), tablet.getId(), schemaHash);
                                batchTask.addTask(alterTask);
                            }
                        }
                    }
                }
            }
        } finally {
            db.readUnlock();
        }

        AgentTaskExecutor.submit(batchTask);
        LOG.info("send clear alter task for table {}, number: {}", olapTable.getName(), batchTask.getTaskNum());
    }

    public void updateTableMeta(Database db, String tableName, Map<String, String> properties,
            TTabletMetaType metaType)
            throws DdlException {
        List<Partition> partitions = Lists.newArrayList();
        OlapTable olapTable;
        db.readLock();
        try {
            olapTable = (OlapTable) db.getTable(tableName);
            partitions.addAll(olapTable.getPartitions());
        } finally {
            db.readUnlock();
        }

        boolean metaValue = false;
        if (metaType == TTabletMetaType.INMEMORY) {
            metaValue = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_INMEMORY));
            if (metaValue == olapTable.isInMemory()) {
                return;
            }
        } else if (metaType == TTabletMetaType.ENABLE_PERSISTENT_INDEX) {
            metaValue = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX));
            if (metaValue == olapTable.enablePersistentIndex()) {
                return;
            }
        } else if (metaType == TTabletMetaType.WRITE_QUORUM) {
            TWriteQuorumType writeQuorum = WriteQuorum
                    .findTWriteQuorumByName(properties.get(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM));
            if (writeQuorum == olapTable.writeQuorum()) {
                return;
            }
        } else if (metaType == TTabletMetaType.REPLICATED_STORAGE) {
            metaValue = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE));
            if (metaValue == olapTable.enableReplicatedStorage()) {
                return;
            }
        } else {
            LOG.warn("meta type: {} does not support", metaType);
            return;
        }

        if (metaType == TTabletMetaType.INMEMORY ||
                metaType == TTabletMetaType.ENABLE_PERSISTENT_INDEX) {
            for (Partition partition : partitions) {
                updatePartitionTabletMeta(db, olapTable.getName(), partition.getName(), metaValue, metaType);
            }
        }

        db.writeLock();
        try {
            GlobalStateMgr.getCurrentState().modifyTableMeta(db, olapTable, properties, metaType);
        } finally {
            db.writeUnlock();
        }
    }

    // return true means that the modification of FEMeta is successful,
    // and as long as the modification of metadata is successful,
    // the final consistency will be achieved through the report handler
    public boolean updateBinlogConfigMeta(Database db, Long tableId, Map<String, String> properties,
            TTabletMetaType metaType) {
        List<Partition> partitions = Lists.newArrayList();
        OlapTable olapTable;
        BinlogConfig newBinlogConfig;
        boolean hasChanged = false;
        boolean isModifiedSuccess = true;
        db.readLock();
        try {
            olapTable = (OlapTable) db.getTable(tableId);
            if (olapTable == null) {
                return false;
            }
            partitions.addAll(olapTable.getPartitions());
            if (!olapTable.containsBinlogConfig()) {
                newBinlogConfig = new BinlogConfig();
                hasChanged = true;
            } else {
                newBinlogConfig = new BinlogConfig(olapTable.getCurBinlogConfig());
            }
        } finally {
            db.readUnlock();
        }

        // judge whether the attribute has changed
        // no exception will be thrown, for the analyzer has checked
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE)) {
            boolean binlogEnable = Boolean.parseBoolean(properties.get(
                    PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE));
            if (binlogEnable != newBinlogConfig.getBinlogEnable()) {
                newBinlogConfig.setBinlogEnable(binlogEnable);
                hasChanged = true;
            }
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL)) {
            Long binlogTtl = Long.parseLong(properties.get(
                    PropertyAnalyzer.PROPERTIES_BINLOG_TTL));
            if (binlogTtl != newBinlogConfig.getBinlogTtlSecond()) {
                newBinlogConfig.setBinlogTtlSecond(binlogTtl);
                hasChanged = true;
            }
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE)) {
            Long binlogMaxSize = Long.parseLong(properties.get(
                    PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE));
            if (binlogMaxSize != newBinlogConfig.getBinlogMaxSize()) {
                newBinlogConfig.setBinlogMaxSize(binlogMaxSize);
                hasChanged = true;
            }
        }
        if (!hasChanged) {
            LOG.info("table {} binlog config is same as the previous version, so nothing need to do", olapTable.getName());
            return true;
        }

        db.writeLock();
        // check for concurrent modifications by version
        if (olapTable.getBinlogVersion() != newBinlogConfig.getVersion()) {
            // binlog config has been modified,
            // no need to judge whether the binlog config is the same as previous one,
            // just modify even if they are the same
            Map<String, String> newProperties = olapTable.getCurBinlogConfig().toProperties();
            newProperties.putAll(properties);
            newBinlogConfig.buildFromProperties(newProperties);
        }
        newBinlogConfig.incVersion();
        try {
            BinlogConfig oldBinlogConfig = olapTable.getCurBinlogConfig();
            GlobalStateMgr.getCurrentState().modifyBinlogMeta(db, olapTable, newBinlogConfig);
            if (oldBinlogConfig != null) {
                LOG.info("update binlog config of table {} successfully, the binlog config after modified is : {}, " +
                                "previous is {}",
                        olapTable.getName(),
                        olapTable.getCurBinlogConfig().toString(),
                        oldBinlogConfig.toString());
            } else {
                LOG.info("update binlog config of table {} successfully, the binlog config after modified is : {}, ",
                        olapTable.getName(), olapTable.getCurBinlogConfig().toString());
            }
        } catch (Exception e) {
            // defensive programming, it normally should not throw an exception,
            // here is just to ensure that a correct result can be returned
            LOG.warn("update binlog config of table {} failed", olapTable.getName());
            isModifiedSuccess = false;
        } finally {
            db.writeUnlock();
        }

        // TODO optimize by asynchronous rpc
        if (metaType != TTabletMetaType.DISABLE_BINLOG) {
            try {
                for (Partition partition : partitions) {
                    updateBinlogPartitionTabletMeta(db, olapTable.getName(), partition.getName(), olapTable.getCurBinlogConfig(),
                            TTabletMetaType.BINLOG_CONFIG);
                }
            } catch (DdlException e) {
                LOG.warn(e);
                return isModifiedSuccess;
            }

        }
        return isModifiedSuccess;
    }

    /**
     * Update some specified partitions' in-memory property of table
     */
    public void updatePartitionsInMemoryMeta(Database db,
            String tableName,
            List<String> partitionNames,
            Map<String, String> properties) throws DdlException {
        OlapTable olapTable;
        db.readLock();
        try {
            olapTable = (OlapTable) db.getTable(tableName);
            for (String partitionName : partitionNames) {
                Partition partition = olapTable.getPartition(partitionName);
                if (partition == null) {
                    throw new DdlException("Partition[" + partitionName + "] does not exist in " +
                            "table[" + olapTable.getName() + "]");
                }
            }
        } finally {
            db.readUnlock();
        }

        boolean isInMemory = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_INMEMORY));
        if (isInMemory == olapTable.isInMemory()) {
            return;
        }

        for (String partitionName : partitionNames) {
            try {
                //updatePartitionInMemoryMeta(db, olapTable.getName(), partitionName, isInMemory);
                updatePartitionTabletMeta(db, olapTable.getName(), partitionName, isInMemory, TTabletMetaType.INMEMORY);
            } catch (Exception e) {
                String errMsg = "Failed to update partition[" + partitionName + "]'s 'in_memory' property. " +
                        "The reason is [" + e.getMessage() + "]";
                throw new DdlException(errMsg);
            }
        }
    }

    /**
     * Update one specified partition's binlog config by partition name of table
     * This operation may return partial successfully, with a exception to inform user to retry
     */
    public void updateBinlogPartitionTabletMeta(Database db,
            String tableName,
            String partitionName,
            BinlogConfig binlogConfig,
            TTabletMetaType metaType) throws DdlException {
        // be id -> <tablet id,schemaHash>
        Map<Long, Set<Pair<Long, Integer>>> beIdToTabletIdWithHash = Maps.newHashMap();
        db.readLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableName);
            Partition partition = olapTable.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException(
                        "Partition[" + partitionName + "] does not exist in table[" + olapTable.getName() + "]");
            }

            MaterializedIndex baseIndex = partition.getBaseIndex();
            int schemaHash = olapTable.getSchemaHashByIndexId(baseIndex.getId());
            for (Tablet tablet : baseIndex.getTablets()) {
                for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                    Set<Pair<Long, Integer>> tabletIdWithHash =
                            beIdToTabletIdWithHash.computeIfAbsent(replica.getBackendId(), k -> Sets.newHashSet());
                    tabletIdWithHash.add(new Pair<>(tablet.getId(), schemaHash));
                }
            }

        } finally {
            db.readUnlock();
        }

        int totalTaskNum = beIdToTabletIdWithHash.keySet().size();
        MarkedCountDownLatch<Long, Set<Pair<Long, Integer>>> countDownLatch = new MarkedCountDownLatch<>(totalTaskNum);
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Map.Entry<Long, Set<Pair<Long, Integer>>> kv : beIdToTabletIdWithHash.entrySet()) {
            countDownLatch.addMark(kv.getKey(), kv.getValue());
            UpdateTabletMetaInfoTask task = new UpdateTabletMetaInfoTask(kv.getKey(), kv.getValue(),
                    binlogConfig, countDownLatch, metaType);
            batchTask.addTask(task);
        }
        if (!FeConstants.runningUnitTest) {
            // send all tasks and wait them finished
            AgentTaskQueue.addBatchTask(batchTask);
            AgentTaskExecutor.submit(batchTask);
            LOG.info("send update tablet meta task for table {}, partitions {}, number: {}",
                    tableName, partitionName, batchTask.getTaskNum());

            // estimate timeout
            long timeout = Config.tablet_create_timeout_second * 1000L * totalTaskNum;
            timeout = Math.min(timeout, Config.max_create_table_timeout_second * 1000L);
            boolean ok = false;
            try {
                ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
            }

            if (!ok || !countDownLatch.getStatus().ok()) {
                String errMsg = "Failed to update partition[" + partitionName + "]. tablet meta.";
                // clear tasks
                AgentTaskQueue.removeBatchTask(batchTask, TTaskType.UPDATE_TABLET_META_INFO);

                if (!countDownLatch.getStatus().ok()) {
                    errMsg += " Error: " + countDownLatch.getStatus().getErrorMsg();
                } else {
                    List<Map.Entry<Long, Set<Pair<Long, Integer>>>> unfinishedMarks = countDownLatch.getLeftMarks();
                    // only show at most 3 results
                    List<Map.Entry<Long, Set<Pair<Long, Integer>>>> subList =
                            unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 3));
                    if (!subList.isEmpty()) {
                        errMsg += " Unfinished mark: " + Joiner.on(", ").join(subList);
                    }
                }
                errMsg += ". This operation maybe partial successfully, You should retry until success.";
                LOG.warn(errMsg);
                throw new DdlException(errMsg);
            }
        }
    }

    /**
     * Update one specified partition's in-memory property by partition name of table
     * This operation may return partial successfully, with a exception to inform user to retry
     */
    public void updatePartitionTabletMeta(Database db,
            String tableName,
            String partitionName,
            boolean metaValue,
            TTabletMetaType metaType) throws DdlException {
        // be id -> <tablet id,schemaHash>
        Map<Long, Set<Pair<Long, Integer>>> beIdToTabletIdWithHash = Maps.newHashMap();
        db.readLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableName);
            Partition partition = olapTable.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException(
                        "Partition[" + partitionName + "] does not exist in table[" + olapTable.getName() + "]");
            }

            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    int schemaHash = olapTable.getSchemaHashByIndexId(index.getId());
                    for (Tablet tablet : index.getTablets()) {
                        for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                            Set<Pair<Long, Integer>> tabletIdWithHash =
                                    beIdToTabletIdWithHash.computeIfAbsent(replica.getBackendId(), k -> Sets.newHashSet());
                            tabletIdWithHash.add(new Pair<>(tablet.getId(), schemaHash));
                        }
                    }
                }
            }
        } finally {
            db.readUnlock();
        }

        int totalTaskNum = beIdToTabletIdWithHash.keySet().size();
        MarkedCountDownLatch<Long, Set<Pair<Long, Integer>>> countDownLatch = new MarkedCountDownLatch<>(totalTaskNum);
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Map.Entry<Long, Set<Pair<Long, Integer>>> kv : beIdToTabletIdWithHash.entrySet()) {
            countDownLatch.addMark(kv.getKey(), kv.getValue());
            UpdateTabletMetaInfoTask task = new UpdateTabletMetaInfoTask(kv.getKey(), kv.getValue(),
                    metaValue, countDownLatch, metaType);
            batchTask.addTask(task);
        }
        if (!FeConstants.runningUnitTest) {
            // send all tasks and wait them finished
            AgentTaskQueue.addBatchTask(batchTask);
            AgentTaskExecutor.submit(batchTask);
            LOG.info("send update tablet meta task for table {}, partitions {}, number: {}",
                    tableName, partitionName, batchTask.getTaskNum());

            // estimate timeout
            long timeout = Config.tablet_create_timeout_second * 1000L * totalTaskNum;
            timeout = Math.min(timeout, Config.max_create_table_timeout_second * 1000L);
            boolean ok = false;
            try {
                ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
            }

            if (!ok || !countDownLatch.getStatus().ok()) {
                String errMsg = "Failed to update partition[" + partitionName + "]. tablet meta.";
                // clear tasks
                AgentTaskQueue.removeBatchTask(batchTask, TTaskType.UPDATE_TABLET_META_INFO);

                if (!countDownLatch.getStatus().ok()) {
                    errMsg += " Error: " + countDownLatch.getStatus().getErrorMsg();
                } else {
                    List<Map.Entry<Long, Set<Pair<Long, Integer>>>> unfinishedMarks = countDownLatch.getLeftMarks();
                    // only show at most 3 results
                    List<Map.Entry<Long, Set<Pair<Long, Integer>>>> subList =
                            unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 3));
                    if (!subList.isEmpty()) {
                        errMsg += " Unfinished mark: " + Joiner.on(", ").join(subList);
                    }
                }
                errMsg += ". This operation maybe partial successfully, You should retry until success.";
                LOG.warn(errMsg);
                throw new DdlException(errMsg);
            }
        }
    }

    public void updateTableConstraint(Database db, String tableName, Map<String, String> properties)
            throws DdlException {
        if (!db.readLockAndCheckExist()) {
            throw new DdlException(String.format("db:%s does not exists.", db.getFullName()));
        }
        TableProperty tableProperty;
        OlapTable olapTable;
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                throw new DdlException(String.format("table:%s does not exist", tableName));
            }
            olapTable = (OlapTable) table;
            tableProperty = olapTable.getTableProperty();
        } finally {
            db.readUnlock();
        }

        boolean hasChanged = false;
        if (tableProperty != null) {
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)) {
                try {
                    List<UniqueConstraint> newUniqueConstraints = PropertyAnalyzer.analyzeUniqueConstraint(properties, db,
                            olapTable);
                    List<UniqueConstraint> originalUniqueConstraints = tableProperty.getUniqueConstraints();
                    if (originalUniqueConstraints == null
                            || !newUniqueConstraints.toString().equals(originalUniqueConstraints.toString())) {
                        hasChanged = true;
                        String newProperty = newUniqueConstraints
                                .stream().map(UniqueConstraint::toString).collect(Collectors.joining(";"));
                        properties.put(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT, newProperty);
                    } else {
                        LOG.warn("unique constraint is the same as origin");
                    }
                } catch (AnalysisException e) {
                    throw new DdlException(
                            String.format("analyze table unique constraint:%s failed",
                                    properties.get(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)), e);
                }
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)) {
                try {
                    List<ForeignKeyConstraint> newForeignKeyConstraints =
                            PropertyAnalyzer.analyzeForeignKeyConstraint(properties, db, olapTable);
                    List<ForeignKeyConstraint> originalForeignKeyConstraints = tableProperty.getForeignKeyConstraints();
                    if (originalForeignKeyConstraints == null
                            || !newForeignKeyConstraints.toString().equals(originalForeignKeyConstraints.toString())) {
                        hasChanged = true;
                        String newProperty = newForeignKeyConstraints
                                .stream().map(ForeignKeyConstraint::toString).collect(Collectors.joining(";"));
                        properties.put(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT, newProperty);
                    } else {
                        LOG.warn("foreign constraint is the same as origin");
                    }
                } catch (AnalysisException e) {
                    throw new DdlException(
                            String.format("analyze table foreign key constraint:%s failed",
                                    properties.get(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)), e);
                }
            }
        }

        if (!hasChanged) {
            return;
        }
        db.writeLock();
        try {
            GlobalStateMgr.getCurrentState().modifyTableConstraint(db, tableName, properties);
        } finally {
            db.writeUnlock();
        }
    }

    @Override
    public void cancel(CancelStmt stmt) throws DdlException {
        CancelAlterTableStmt cancelAlterTableStmt = (CancelAlterTableStmt) stmt;

        String dbName = cancelAlterTableStmt.getDbName();
        String tableName = cancelAlterTableStmt.getTableName();
        Preconditions.checkState(!Strings.isNullOrEmpty(dbName));
        Preconditions.checkState(!Strings.isNullOrEmpty(tableName));

        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database[" + dbName + "] does not exist");
        }

        AlterJobV2 schemaChangeJobV2 = null;
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }
            if (!(table instanceof OlapTable)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NOT_OLAP_TABLE, tableName);
            }
            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getState() != OlapTableState.SCHEMA_CHANGE
                    && olapTable.getState() != OlapTableState.WAITING_STABLE) {
                throw new DdlException("Table[" + tableName + "] is not under SCHEMA_CHANGE/WAITING_STABLE.");
            }

            // find from new alter jobs first
            List<AlterJobV2> schemaChangeJobV2List = getUnfinishedAlterJobV2ByTableId(olapTable.getId());
            // current schemaChangeJob job doesn't support batch operation,so just need to get one job
            schemaChangeJobV2 =
                    schemaChangeJobV2List.size() == 0 ? null : Iterables.getOnlyElement(schemaChangeJobV2List);
            if (schemaChangeJobV2 == null) {
                throw new DdlException(
                        "Table[" + tableName + "] is under SCHEMA_CHANGE but job does not exits.");
            }
        } finally {
            db.writeUnlock();
        }

        // alter job v2's cancel must be called outside the database lock
        if (!schemaChangeJobV2.cancel("user cancelled")) {
            throw new DdlException("Job can not be cancelled. State: " + schemaChangeJobV2.getJobState());
        }
    }

    private void processAddIndex(CreateIndexClause alterClause, OlapTable olapTable, List<Index> newIndexes)
            throws UserException {
        if (alterClause.getIndex() == null) {
            return;
        }

        List<Index> existedIndexes = olapTable.getIndexes();
        IndexDef indexDef = alterClause.getIndexDef();
        Set<String> newColset = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        newColset.addAll(indexDef.getColumns());
        for (Index existedIdx : existedIndexes) {
            if (existedIdx.getIndexName().equalsIgnoreCase(indexDef.getIndexName())) {
                throw new DdlException("index `" + indexDef.getIndexName() + "` already exist.");
            }
            Set<String> existedIdxColSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            existedIdxColSet.addAll(existedIdx.getColumns());
            if (newColset.equals(existedIdxColSet)) {
                throw new DdlException(
                        "index for columns (" + String.join(",", indexDef.getColumns()) + " ) already exist.");
            }
        }

        for (String col : indexDef.getColumns()) {
            Column column = olapTable.getColumn(col);
            if (column != null) {
                indexDef.checkColumn(column, olapTable.getKeysType());
            } else {
                throw new DdlException("BITMAP column does not exist in table. invalid column: " + col);
            }
        }

        newIndexes.add(alterClause.getIndex());
    }

    private void processDropIndex(DropIndexClause alterClause, OlapTable olapTable, List<Index> indexes)
            throws DdlException {
        String indexName = alterClause.getIndexName();
        List<Index> existedIndexes = olapTable.getIndexes();
        Index found = null;
        for (Index existedIdx : existedIndexes) {
            if (existedIdx.getIndexName().equalsIgnoreCase(indexName)) {
                found = existedIdx;
                break;
            }
        }
        if (found == null) {
            throw new DdlException("index " + indexName + " does not exist");
        }

        Iterator<Index> itr = indexes.iterator();
        while (itr.hasNext()) {
            Index idx = itr.next();
            if (idx.getIndexName().equalsIgnoreCase(alterClause.getIndexName())) {
                itr.remove();
                break;
            }
        }
    }

    // the invoker should keep table's write lock
    public void modifyTableAddOrDropColumns(Database db, OlapTable olapTable,
            Map<Long, LinkedList<Column>> indexSchemaMap,
            List<Index> indexes, long jobId, boolean isReplay)
            throws DdlException, NotImplementedException {
        try {
            db.writeLock();

            LOG.debug("indexSchemaMap:{}, indexes:{}", indexSchemaMap, indexes);
            if (olapTable.getState() == OlapTableState.ROLLUP) {
                throw new DdlException("Table[" + olapTable.getName() + "]'s is doing ROLLUP job");
            }

            // for now table's state can only be NORMAL
            Preconditions.checkState(olapTable.getState() == OlapTableState.NORMAL, olapTable.getState().name());

            // for bitmapIndex
            boolean hasIndexChange = false;
            Set<Index> newSet = new HashSet<>(indexes);
            Set<Index> oriSet = new HashSet<>(olapTable.getIndexes());
            if (!newSet.equals(oriSet)) {
                hasIndexChange = true;
            }

            // begin checking each table
            // ATTN: DO NOT change any meta in this loop
            Map<Long, List<Column>> changedIndexIdToSchema = Maps.newHashMap();
            for (Long alterIndexId : indexSchemaMap.keySet()) {
                // Must get all columns including invisible columns.
                // Because in alter process, all columns must be considered.
                List<Column> alterSchema = indexSchemaMap.get(alterIndexId);

                LOG.debug("index[{}] is changed. start checking...", alterIndexId);
                // 1. check order: a) has key; b) value after key
                boolean meetValue = false;
                boolean hasKey = false;
                for (Column column : alterSchema) {
                    if (column.isKey() && meetValue) {
                        throw new DdlException(
                                "Invalid column order. value should be after key. index[" + olapTable.getIndexNameById(
                                        alterIndexId) + "]");
                    }
                    if (!column.isKey()) {
                        meetValue = true;
                    } else {
                        hasKey = true;
                    }
                }
                if (!hasKey) {
                    throw new DdlException("No key column left. index[" + olapTable.getIndexNameById(alterIndexId) + "]");
                }

                // 2. check partition key
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                if (partitionInfo.getType() == PartitionType.RANGE || partitionInfo.getType() == PartitionType.LIST) {
                    List<Column> partitionColumns = partitionInfo.getPartitionColumns();
                    for (Column partitionCol : partitionColumns) {
                        boolean found = false;
                        for (Column alterColumn : alterSchema) {
                            if (alterColumn.nameEquals(partitionCol.getName(), true)) {
                                found = true;
                                break;
                            }
                        } // end for alterColumns

                        if (!found && alterIndexId == olapTable.getBaseIndexId()) {
                            // 2.1 partition column cannot be deleted.
                            throw new DdlException(
                                    "Partition column[" + partitionCol.getName() + "] cannot be dropped. index["
                                            + olapTable.getIndexNameById(alterIndexId) + "]");
                        }
                    } // end for partitionColumns
                }

                // 3. check distribution key:
                DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
                if (distributionInfo.getType() == DistributionInfoType.HASH) {
                    List<Column> distributionColumns = ((HashDistributionInfo) distributionInfo).getDistributionColumns();
                    for (Column distributionCol : distributionColumns) {
                        boolean found = false;
                        for (Column alterColumn : alterSchema) {
                            if (alterColumn.nameEquals(distributionCol.getName(), true)) {
                                found = true;
                                break;
                            }
                        } // end for alterColumns
                        if (!found && alterIndexId == olapTable.getBaseIndexId()) {
                            // 2.2 distribution column cannot be deleted.
                            throw new DdlException(
                                    "Distribution column[" + distributionCol.getName() + "] cannot be dropped. index["
                                            + olapTable.getIndexNameById(alterIndexId) + "]");
                        }
                    } // end for distributionCols
                }

                // 5. store the changed columns for edit log
                changedIndexIdToSchema.put(alterIndexId, alterSchema);

                LOG.debug("schema change[{}-{}-{}] check pass.", db.getId(), olapTable.getId(), alterIndexId);
            } // end for indices

            if (changedIndexIdToSchema.isEmpty() && !hasIndexChange) {
                throw new DdlException("Nothing is changed. please check your alter stmt.");
            }

            // update base index schema
            long baseIndexId = olapTable.getBaseIndexId();
            List<Long> indexIds = new ArrayList<Long>();
            indexIds.add(baseIndexId);
            indexIds.addAll(olapTable.getIndexIdListExceptBaseIndex());
            for (int i = 0; i < indexIds.size(); i++) {
                List<Column> indexSchema = indexSchemaMap.get(indexIds.get(i));
                MaterializedIndexMeta currentIndexMeta = olapTable.getIndexMetaByIndexId(indexIds.get(i));
                currentIndexMeta.setSchema(indexSchema);

                int currentSchemaVersion = currentIndexMeta.getSchemaVersion();
                int newSchemaVersion = currentSchemaVersion + 1;
                currentIndexMeta.setSchemaVersion(newSchemaVersion);
            }
            olapTable.setIndexes(indexes);
            olapTable.rebuildFullSchema();

            // update max column unique id
            int maxColUniqueId = olapTable.getMaxColUniqueId();
            for (Column column : indexSchemaMap.get(olapTable.getBaseIndexId())) {
                if (column.getUniqueId() > maxColUniqueId) {
                    maxColUniqueId = column.getUniqueId();
                }
            }
            olapTable.setMaxColUniqueId(maxColUniqueId);

            if (!isReplay) {
                TableAddOrDropColumnsInfo info = new TableAddOrDropColumnsInfo(db.getId(), olapTable.getId(),
                        indexSchemaMap, indexes, jobId);
                LOG.debug("logModifyTableAddOrDropColumns info:{}", info);
                GlobalStateMgr.getCurrentState().getEditLog().logModifyTableAddOrDropColumns(info);
            }

            SchemaChangeJobV2 schemaChangeJob = new SchemaChangeJobV2(jobId, db.getId(), olapTable.getId(),
                    olapTable.getName(), 1000);
            schemaChangeJob.setJobState(AlterJobV2.JobState.FINISHED);
            schemaChangeJob.setFinishedTimeMs(System.currentTimeMillis());
            this.addAlterJobV2(schemaChangeJob);

            LOG.info("finished modify table's add or drop columns. table: {}, is replay: {}", olapTable.getName(),
                    isReplay);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayModifyTableAddOrDropColumns(TableAddOrDropColumnsInfo info) throws
            MetaNotFoundException {
        LOG.debug("info:{}", info);
        long dbId = info.getDbId();
        long tableId = info.getTableId();
        Map<Long, LinkedList<Column>> indexSchemaMap = info.getIndexSchemaMap();
        List<Index> indexes = info.getIndexes();
        long jobId = info.getJobId();

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        Table table = db.getTable(tableId);
        Preconditions.checkArgument(table instanceof OlapTable,
                "Target of light schema change must be olap table");
        OlapTable olapTable = (OlapTable) table;
        try {
            db.writeLock();
            modifyTableAddOrDropColumns(db, olapTable, indexSchemaMap, indexes, jobId, true);
        } catch (DdlException e) {
            // should not happen
            LOG.warn("failed to replay modify table add or drop columns", e);
        } catch (NotImplementedException e) {
            LOG.error("InternalError", e);
        } finally {
            db.writeUnlock();
        }
    }
}
