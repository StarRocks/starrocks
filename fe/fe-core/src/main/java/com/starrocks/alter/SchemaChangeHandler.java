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
import com.starrocks.analysis.BloomFilterIndexUtil;
import com.starrocks.analysis.ColumnPosition;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
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
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.NotImplementedException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.WriteQuorum;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.TableAddOrDropColumnsInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddFieldClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableColumnClause;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.CancelStmt;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropFieldClause;
import com.starrocks.sql.ast.DropIndexClause;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.IndexDef.IndexType;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.OptimizeClause;
import com.starrocks.sql.ast.ReorderColumnsClause;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.TabletMetadataUpdateAgentTask;
import com.starrocks.task.TabletMetadataUpdateAgentTaskFactory;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TWriteQuorumType;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.AbstractMap.SimpleEntry;
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
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.inactiveRelatedMaterializedViews;

public class SchemaChangeHandler extends AlterHandler {

    private static final Logger LOG = LogManager.getLogger(SchemaChangeHandler.class);

    // all shadow indexes should have this prefix in name
    public static final String SHADOW_NAME_PREFIX = "__starrocks_shadow_";

    public SchemaChangeHandler() {
        super("schema change");
    }

    private AlterJobV2 createOptimizeTableJob(
            OptimizeClause optimizeClause, Database db, OlapTable olapTable, Map<String, String> propertyMap)
            throws UserException {
        if (olapTable.getState() != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + olapTable.getName() + "]'s is not in NORMAL state");
        }

        long timeoutSecond = PropertyAnalyzer.analyzeTimeout(propertyMap, Config.alter_table_timeout_second);

        // create job
        OptimizeJobV2Builder jobBuilder = olapTable.optimizeTable();
        jobBuilder.withOptimizeClause(optimizeClause)
                .withJobId(GlobalStateMgr.getCurrentState().getNextId())
                .withDbId(db.getId())
                .withTimeoutSeconds(timeoutSecond)
                .withWarehouse(ConnectContext.get().getCurrentWarehouseId());

        return jobBuilder.build();
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
        // only new table generate ColUniqueId, exist table do not.
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

        boolean ligthSchemaChange = olapTable.getUseFastSchemaEvolution();
        if (alterClause.getGeneratedColumnPos() == null) {
            for (Column column : columns) {
                ligthSchemaChange &= addColumnInternal(olapTable, column, null, targetIndexId, baseIndexId, indexSchemaMap,
                        newColNameSet);
            }
        } else {
            for (int i = columns.size() - 1; i >= 0; --i) {
                Column column = columns.get(i);
                addColumnInternal(olapTable, column, alterClause.getGeneratedColumnPos(),
                        targetIndexId, baseIndexId, indexSchemaMap, newColNameSet);
                // add a generated column need to rewrite data, can not use light schema change
                ligthSchemaChange = false;
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
        boolean fastSchemaEvolution = olapTable.getUseFastSchemaEvolution();
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
            long baseIndexId = olapTable.getBaseIndexId();
            List<Column> baseSchema = indexSchemaMap.get(baseIndexId);
            boolean isKey = baseSchema.stream().anyMatch(c -> c.isKey() && c.getName().equalsIgnoreCase(dropColName));
            if (isKey) {
                throw new DdlException("Can not drop key column in primary data model table");
            }
            fastSchemaEvolution &= !isKey;
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
            fastSchemaEvolution &= !isKey;
            if (isKey) {
                throw new DdlException("Can not drop key column in Unique data model table");
            }
        } else if (KeysType.AGG_KEYS == olapTable.getKeysType()) {
            if (null == targetIndexName) {
                // drop column in base table
                long baseIndexId = olapTable.getBaseIndexId();
                List<Column> baseSchema = indexSchemaMap.get(baseIndexId);
                boolean isKey = baseSchema.stream().anyMatch(c -> c.isKey() && c.getName().equalsIgnoreCase(dropColName));
                fastSchemaEvolution &= !isKey;
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
                fastSchemaEvolution &= !isKey;
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
                    fastSchemaEvolution = false;
                    break;
                }
            }
        }

        Column droppedColumn = olapTable.getColumn(dropColName);
        // Remove all Index that contains a column with the name dropColName.
        if (droppedColumn != null) {
            indexes.removeIf(index -> index.getColumns()
                    .stream()
                    .anyMatch(c -> c.equalsIgnoreCase(droppedColumn.getColumnId())));
        }

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
                        fastSchemaEvolution = false;
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
                            fastSchemaEvolution = false;
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
                        fastSchemaEvolution = false;
                    }
                }
            }

            if (!removed) {
                throw new DdlException("Column does not exists: " + dropColName);
            }
        }
        return fastSchemaEvolution;
    }

    // Get specified type of modfyColumn according to nestFieldName
    // Only support Struct column right now. If the column is array column, the element type must be struct and
    // we will use '[*]' to represent all elements in the array
    // The nestedFieldName stores field names hierarchically. When searching, follow the nestedFieldName step by step.
    // e.g.
    //   column: Struct<c1 int, c2 Struct<c3 int, c4 Array<Struct<c5 int, c6 int>>>>
    //   1. nestedFieldName: {c1}
    //            Find `c1` and return Type INT
    //   2. nestedFieldName: {c2, c3}
    //            Find `c3` and return Type INT
    //   3. nestedFieldName: {c2, c4, [*]}
    //            Find element in `c4` and return Type Struct<INT, INT>
    //   3. nestedFieldName: {c2, c4, [*], c6}
    //            Find `c6` and return Type INT
    private Type getModifiedType(Column modifyColumn, List<String> nestedFieldName) throws DdlException {
        Type modifyFieldType = modifyColumn.getType();
        String modifyFieldName = modifyColumn.getName();

        if (nestedFieldName != null && !nestedFieldName.isEmpty()) {
            Function<Map.Entry<Type, String>, Type> getFieldType = entry -> {
                Type fieldType = entry.getKey();
                String name = entry.getValue();
                if (fieldType.isStructType()) {
                    StructField field = ((StructType) fieldType).getField(name);
                    if (field == null) {
                        return null;
                    } else {
                        return field.getType();
                    }
                } else {
                    return ((ArrayType) fieldType).getItemType();
                }
            };

            modifyFieldType = modifyColumn.getType();
            for (int i = 0; i < nestedFieldName.size(); i++) {
                Map.Entry<Type, String> entry = new SimpleEntry<>(modifyFieldType, nestedFieldName.get(i));
                modifyFieldType = getFieldType.apply(entry);
                if (modifyFieldType == null) {
                    throw new DdlException("Field[" + nestedFieldName.get(i) + "] not exist in Field[" + modifyFieldName + "]");
                }
                modifyFieldName = nestedFieldName.get(i);
                if (!modifyFieldType.isStructType() && !modifyFieldType.isArrayType()) {
                    throw new DdlException("Field " + modifyFieldName + " is invalid, add/drop field only support for Struct");
                }
            }
        }

        return modifyFieldType;
    }

    /**
     * @param alterClause
     * @param olapTable
     * @param indexSchemaMap
     * @param id
     * @return void
     * @throws DdlException
     */
    private void processAddField(AddFieldClause alterClause, OlapTable olapTable,
                                 Map<Long, LinkedList<Column>> indexSchemaMap,
                                 int id, List<Index> indexes) throws DdlException {
        String modifyColumnName = alterClause.getColName();

        // Struct column can not be key column or sort key column right now. 
        long baseIndexId = olapTable.getBaseIndexId();
        List<Column> baseSchema = indexSchemaMap.get(baseIndexId);
        Optional<Column> col = baseSchema.stream().filter(c -> c.getName().equalsIgnoreCase(modifyColumnName)).findFirst();
        if (!col.isPresent()) {
            throw new DdlException("Column[" + modifyColumnName + "] not exists");
        }
        // we will modify the field type of col, so we deep copy a new modifyColumn to prevent corrupt memory
        Column modifyColumn = col.get().deepCopy();
        if (!modifyColumn.getType().isStructType() && !modifyColumn.getType().isArrayType()) {
            throw new DdlException("Column[" + modifyColumnName + "] type is invalid, add field only support Struct");
        }

        List<String> nestedParentFieldNames = alterClause.getNestedParentFieldNames();
        Type modifyFieldType = getModifiedType(modifyColumn, nestedParentFieldNames);
        String modifyFieldName = null;
        // If nestedParentFieldNames is empty, this means we add a new field directly to this column. 
        // Otherwise, it means adding a new field to the specified field within the column.
        // But the specified field type must be Struct.
        if (nestedParentFieldNames == null || nestedParentFieldNames.isEmpty()) {
            modifyFieldName = modifyColumn.getName();
        } else {
            modifyFieldName = nestedParentFieldNames.get(nestedParentFieldNames.size() - 1);
        }
        if (!modifyFieldType.isStructType()) {
            throw new DdlException("Field " + modifyFieldName + " is invalid, add field only support for Struct");
        }

        // Remove all Index that contains a column with the name modifyColumnName
        indexes.removeIf(index -> index.getColumns().stream().anyMatch(c -> c.equalsIgnoreCase(col.get().getColumnId())));
        // Add new field into the pecified field. If no `fieldPos`, the new field will be added to the end.
        StructType oriFieldType = ((StructType) modifyFieldType);
        String fieldName = alterClause.getFieldName();
        Type fieldType = alterClause.getType();
        StructField addField = new StructField(fieldName, id, fieldType, null);
        ColumnPosition fieldPos = alterClause.getFieldPos();
        ArrayList<StructField> oriFields = oriFieldType.getFields();
        int posIndex = -1;
        boolean hasPos = (fieldPos != null);
        if (fieldPos != null) {
            if (fieldPos.isFirst()) {
                posIndex = 0;
            } else {
                if (oriFieldType.getField(fieldPos.getLastCol()) == null) {
                    throw new DdlException("Field[" + fieldPos.getLastCol() + "] is not exist in Field[" + modifyFieldName + "]");
                }
                posIndex = oriFieldType.getFieldPos(fieldPos.getLastCol()) + 1;
            }
        } else {
            posIndex = oriFields.size();
        }

        // update the modifyColumn int index schema.
        ArrayList<StructField> fields = new ArrayList<>();
        for (StructField field : oriFieldType.getFields()) {
            fields.add(field);
        }
        fields.add(posIndex, addField);
        oriFieldType.updateFields(fields);
        for (Map.Entry<Long, LinkedList<Column>> entry : indexSchemaMap.entrySet()) {
            List<Column> modIndexSchema = entry.getValue();
            Optional<Column> oneCol = modIndexSchema.stream().filter(c -> c.nameEquals(modifyColumnName, true)).findFirst();
            if (!oneCol.isPresent()) {
                continue;
            } else {
                int idx = modIndexSchema.indexOf(oneCol.get());
                modIndexSchema.set(idx, modifyColumn);
            }
        }
    }

    /**
     * @param alterClause
     * @param olapTable
     * @param indexSchemaMap
     * @return void
     * @throws DdlException
     */
    private void processDropField(DropFieldClause alterClause, OlapTable olapTable,
                                  Map<Long, LinkedList<Column>> indexSchemaMap,
                                  List<Index> indexes) throws DdlException {
        String modifyColumnName = alterClause.getColName();
        String baseIndexName = olapTable.getName();

        // Struct column can not be key column or sort key column right now. 
        long baseIndexId = olapTable.getBaseIndexId();
        List<Column> baseSchema = indexSchemaMap.get(baseIndexId);
        Optional<Column> col = baseSchema.stream().filter(c -> c.getName().equalsIgnoreCase(modifyColumnName)).findFirst();
        if (!col.isPresent()) {
            throw new DdlException("Column[" + modifyColumnName + "] not exists");
        }
        // we will modify the field type of col, so we deep copy a new modifyColumn to prevent corrupt memory
        Column modifyColumn = col.get().deepCopy();
        if (!modifyColumn.getType().isStructType() && !modifyColumn.getType().isArrayType()) {
            throw new DdlException("Column[" + modifyColumnName + "] type is invalid, drop field only support Struct");
        }

        // If nestedFieldName is empty, this means we drop a field directly from this column. 
        // Otherwise, it means dropping a field from the specified field within the column.
        // But the specified field type must be Struct.
        List<String> nestedFieldName = alterClause.getNestedParentFieldNames();
        Type modifyFieldType = getModifiedType(modifyColumn, nestedFieldName);
        String modifyFieldName = null;
        if (nestedFieldName == null || nestedFieldName.isEmpty()) {
            modifyFieldName = modifyColumn.getName();
        } else {
            modifyFieldName = nestedFieldName.get(nestedFieldName.size() - 1);
        }
        if (!modifyFieldType.isStructType()) {
            throw new DdlException("Field " + modifyFieldName + " is invalid, drop field only support for Struct");
        }

        StructType oriFieldType = ((StructType) modifyFieldType);
        String dropFieldName = alterClause.getFieldName();
        if (oriFieldType.getField(dropFieldName) == null) {
            throw new DdlException("Field[" + dropFieldName + "] is not exist in Field[" + modifyFieldName + "]");
        }

        // Remove all Index that contains a column with the name modifyColumnName
        indexes.removeIf(index -> index.getColumns().stream().anyMatch(c -> c.equalsIgnoreCase(col.get().getColumnId())));
        // remove the dropped field from fields and update StructFields
        ArrayList<StructField> fields = new ArrayList<>();
        for (StructField field : oriFieldType.getFields()) {
            if (field.getName().equalsIgnoreCase(dropFieldName)) {
                continue;
            }
            fields.add(field);
        }
        if (fields.isEmpty()) {
            throw new DdlException("Field[" + dropFieldName + "] is the last field of column[" + modifyColumnName +
                    "], can not drop any more.");
        }
        oriFieldType.updateFields(fields);

        // update the modifyColumn int index schema.
        for (Map.Entry<Long, LinkedList<Column>> entry : indexSchemaMap.entrySet()) {
            List<Column> modIndexSchema = entry.getValue();
            Optional<Column> oneCol = modIndexSchema.stream().filter(c -> c.nameEquals(modifyColumnName, true)).findFirst();
            if (!oneCol.isPresent()) {
                continue;
            } else {
                int idx = modIndexSchema.indexOf(oneCol.get());
                modIndexSchema.set(idx, modifyColumn);
            }
        }
    }

    // User can modify column type and column position
    private boolean processModifyColumn(ModifyColumnClause alterClause, OlapTable olapTable,
                                        Map<Long, LinkedList<Column>> indexSchemaMap) throws DdlException {
        // The fast schema evolution mechanism is only supported for modified columns in shared data mode.
        boolean fastSchemaEvolution = RunMode.isSharedDataMode() && olapTable.getUseFastSchemaEvolution();
        Column modColumn = alterClause.getColumn();
        if (KeysType.PRIMARY_KEYS == olapTable.getKeysType()) {
            if (olapTable.getBaseColumn(modColumn.getName()) != null && olapTable.getBaseColumn(modColumn.getName()).isKey()) {
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
            if (modColumn.getAggregationType() != null && modColumn.getAggregationType() != AggregateType.REPLACE) {
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

        if (!modColumn.isGeneratedColumn() && olapTable.hasGeneratedColumn()) {
            for (Column column : olapTable.getFullSchema()) {
                if (!column.isGeneratedColumn()) {
                    continue;
                }
                List<SlotRef> slots = column.getGeneratedColumnRef(olapTable.getIdToColumn());
                for (SlotRef slot : slots) {
                    if (slot.getColumnName().equals(modColumn.getName())) {
                        throw new DdlException("Do not support modify column: " + modColumn.getName() +
                                ", because it associates with the generated column");
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

        for (Index index : olapTable.getIndexes()) {
            if (index.getIndexType() == IndexDef.IndexType.GIN) {
                if (index.getColumns().contains(oriColumn.getColumnId()) &&
                        !modColumn.getType().isStringType()) {
                    throw new DdlException("Cannot modify a column with GIN into non-string type");
                }
            } else if (index.getIndexType() == IndexType.VECTOR) {
                if (index.getColumns().contains(oriColumn.getColumnId()) && modColumn.getType() != oriColumn.getType()) {
                    throw new DdlException("Cannot modify a column with VECTOR index");
                }
            }
        }

        if (oriColumn.isAutoIncrement()) {
            throw new DdlException("Can't not modify a AUTO_INCREMENT column");
        }

        // retain old column name
        modColumn.setName(oriColumn.getName());
        modColumn.setUniqueId(oriColumn.getUniqueId());

        if (!oriColumn.isGeneratedColumn() && modColumn.isGeneratedColumn()) {
            throw new DdlException("Can not modify a non-generated column to a generated column");
        }

        if (oriColumn.isGeneratedColumn() && !modColumn.isGeneratedColumn()) {
            throw new DdlException("Can not modify a generated column to a non-generated column");
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

            String originalModColumnName = modColumn.getName();
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
                modColumn.setName(SHADOW_NAME_PREFIX + originalModColumnName);
            }

            if (KeysType.AGG_KEYS == olapTable.getKeysType() || KeysType.UNIQUE_KEYS == olapTable.getKeysType() ||
                    KeysType.PRIMARY_KEYS == olapTable.getKeysType()) {
                for (Long otherIndexId : otherIndexIds) {
                    List<Column> otherIndexSchema = indexSchemaMap.get(otherIndexId);
                    modColIndex = -1;
                    for (int i = 0; i < otherIndexSchema.size(); i++) {
                        if (otherIndexSchema.get(i).getName().equalsIgnoreCase(originalModColumnName)) {
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
                        if (otherIndexSchema.get(i).getName().equalsIgnoreCase(originalModColumnName)) {
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

        // fast schema evolution supports the conversion of scalar types to decimal types, but does not support the conversion
        // of decimal types to other scale types, due to the fact that the precision and scale of the decimal are not recorded
        // in the segment file
        if (modColumn.isKey() || !modColumn.getType().isScalarType()
                || oriColumn.isKey()
                || !oriColumn.getType().isScalarType()
                || oriColumn.getType().isDecimalOfAnyVersion()) {
            fastSchemaEvolution = false;
        }
        return fastSchemaEvolution;
    }

    // Because modifying the sort key columns and reordering table schema use the same syntax(Alter table xxx ORDER BY(...))
    // And reordering table schema need to provide all columns, so we use the number of columns in the alterClause to determine
    // whether it's modifying the sorting columns or reordering the table schema
    private boolean changeSortKeyColumn(ReorderColumnsClause alterClause, OlapTable table) throws DdlException {
        List<String> orderedColumns = alterClause.getColumnsByPos();
        List<Column> baseSchema = table.getBaseSchema();
        return (orderedColumns.size() != baseSchema.size());
    }

    private void processReorderColumn(ReorderColumnsClause alterClause, OlapTable olapTable,
                                      Map<Long, LinkedList<Column>> indexSchemaMap) throws DdlException {
        if (olapTable.getKeysType() == KeysType.PRIMARY_KEYS) {
            throw new DdlException("Primary key table do not support reorder table schema. Please confirm if you want to " +
                    "modify the sorting columns.");
        }
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

    private void processModifySortKeyColumn(ReorderColumnsClause alterClause, OlapTable olapTable,
                                            Map<Long, LinkedList<Column>> indexSchemaMap, List<Integer> sortKeyIdxes,
                                            List<Integer> sortKeyUniqueIds) throws DdlException {
        LinkedList<Column> targetIndexSchema = indexSchemaMap.get(olapTable.getIndexIdByName(olapTable.getName()));
        // check sort key column list
        Set<String> colNameSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

        boolean useSortKeyUniqueId = true;
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
            if (useSortKeyUniqueId && oneCol.get().getUniqueId() > Column.COLUMN_UNIQUE_ID_INIT_VALUE) {
                sortKeyUniqueIds.add(oneCol.get().getUniqueId());
            } else {
                useSortKeyUniqueId = false;
                sortKeyUniqueIds.clear();
            }
        }

        List<Integer> keyColumnIdxes = new ArrayList<>();
        int columnId = 0;
        for (Column column : targetIndexSchema) {
            if (column.isKey()) {
                keyColumnIdxes.add(columnId);
            }
            columnId++;
        }
        if (olapTable.getKeysType() == KeysType.DUP_KEYS) {
            // duplicate table has no limit in sort key columns
        } else if (olapTable.getKeysType() == KeysType.PRIMARY_KEYS) {
            // sort key column of primary key table has type limitation
            for (int sortKeyIdx : sortKeyIdxes) {
                Column col = targetIndexSchema.get(sortKeyIdx);
                Type t = col.getType();
                if (!(t.isBoolean() || t.isIntegerType() || t.isLargeint() || t.isVarchar() || t.isDate() ||
                        t.isDatetime())) {
                    throw new DdlException("Sort key column[" + col.getName() + "] type not supported: " + t +
                            " in PrimaryKey table");
                }
            }
        } else if (olapTable.getKeysType() == KeysType.AGG_KEYS || olapTable.getKeysType() == KeysType.UNIQUE_KEYS) {
            // sort key column must include all key columns and can not have any other columns
            boolean res = new HashSet<>(keyColumnIdxes).equals(new HashSet<>(sortKeyIdxes));
            if (!res) {
                throw new DdlException("The sort columns of " + olapTable.getKeysType().toSql() +
                        " table must be same with key columns");
            }
        } else {
            throw new DdlException("Table type:" + olapTable.getKeysType().toSql() + " does not support sort key column");
        }
    }

    /**
     * @param olapTable
     * @param newColumn      Add 'newColumn' to specified index.
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
        if (defaultValueType != Column.DefaultValueType.CONST && defaultValueType != Column.DefaultValueType.NULL) {
            throw new DdlException("unsupported default expr:" + newColumn.getDefaultExpr().getExpr());
        }

        boolean fastSchemaEvolution = olapTable.getUseFastSchemaEvolution();
        // if column is generated column, need to rewrite table data, so we can not use light schema change
        if (newColumn.isAutoIncrement() || newColumn.isGeneratedColumn()) {
            fastSchemaEvolution = false;
        }

        if (newColumn.getDefaultValue() != null && newColumn.getDefaultExpr() != null) {
            fastSchemaEvolution = false;
        }
        if (newColumn.getDefaultExpr() != null && newColumn.getDefaultValueType() == Column.DefaultValueType.CONST) {
            long startTime = ConnectContext.get().getStartTime();
            newColumn.setDefaultValue(newColumn.calculatedDefaultValueWithTime(startTime));
        }

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
            if (newColumn.getAggregationType() != null && newColumn.getAggregationType() != AggregateType.REPLACE) {
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
            fastSchemaEvolution = false;
        }

        // check if the new column already exist in base schema.
        // do not support adding new column which already exist in base schema.
        Optional<Column> foundColumn = olapTable.getBaseSchema().stream()
                .filter(c -> c.getName().equalsIgnoreCase(newColName)).findFirst();
        if (foundColumn.isPresent() && newColumn.equals(foundColumn.get())) {
            throw new DdlException(
                    "Can not add column which already exists in base table: " + newColName);
        }

        // check if the new column already exist in column id.
        // do not support adding new column which already exist in column id.
        foundColumn = olapTable.getBaseSchema().stream()
                .filter(c -> c.getColumnId().getId().equalsIgnoreCase(newColName)).findFirst();
        if (foundColumn.isPresent()) {
            throw new DdlException(
                    "Can not add column which already exists in column id: " + newColName
                            + ", you can remove " + foundColumn.get() + " and try again.");
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
                    return fastSchemaEvolution;
                }
                // 2. add to rollup
                fastSchemaEvolution = false;
                modIndexSchema = indexSchemaMap.get(targetIndexId);
                checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, false);
            }
        } else if (KeysType.DUP_KEYS == olapTable.getKeysType()) {
            if (targetIndexId == -1L) {
                // add to base index
                List<Column> modIndexSchema = indexSchemaMap.get(baseIndexId);
                checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, true);
                // no specified target index. return
                return fastSchemaEvolution;
            } else {
                // add to rollup index
                fastSchemaEvolution = false;
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
                return fastSchemaEvolution;
            }

            fastSchemaEvolution = false;
            // 2. add to rollup index
            modIndexSchema = indexSchemaMap.get(targetIndexId);
            checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, false);
        }
        return fastSchemaEvolution;
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

        if (hasPos && modIndexSchema.get(posIndex) != null) {
            Column posColumn = modIndexSchema.get(posIndex);
            if (posColumn.isGeneratedColumn()) {
                throw new DdlException("Can not add column after Generated Column");
            }
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

    private SchemaChangeData finalAnalyze(Database db, OlapTable olapTable,
                                          Map<Long, LinkedList<Column>> indexSchemaMap,
                                          Map<String, String> propertyMap,
                                          List<Index> indexes,
                                          Set<String> modifyFieldColumns) throws UserException {
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
        Set<String> oriBfColumns = olapTable.getBfColumnNames();
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

        Set<ColumnId> bfColumnIds = null;
        if (bfColumns != null) {
            bfColumnIds = Sets.newTreeSet(ColumnId.CASE_INSENSITIVE_ORDER);
            for (String columnName : bfColumns) {
                Column column = olapTable.getColumn(columnName);
                if (column == null) {
                    throw new DdlException("can not find column by name: " + columnName);
                }
                bfColumnIds.add(column.getColumnId());
            }
        }

        BloomFilterIndexUtil.analyseBfWithNgramBf(olapTable, newSet, bfColumnIds);

        // property 3: timeout
        long timeoutSecond = PropertyAnalyzer.analyzeTimeout(propertyMap, Config.alter_table_timeout_second);

        // create job
        SchemaChangeData.Builder dataBuilder = SchemaChangeData.newBuilder();
        dataBuilder.withDatabase(db)
                .withTable(olapTable)
                .withTimeoutInSeconds(timeoutSecond)
                .withAlterIndexInfo(hasIndexChange, indexes)
                .withBloomFilterColumns(bfColumnIds, bfFpp)
                .withBloomFilterColumnsChanged(hasBfChange);

        if (RunMode.isSharedDataMode()) {
            // check warehouse
            long warehouseId = ConnectContext.get().getCurrentWarehouseId();
            List<Long> computeNodeIs = GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllComputeNodeIds(warehouseId);
            if (computeNodeIs.isEmpty()) {
                Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseId);
                throw new DdlException("no available compute nodes in warehouse " + warehouse.getName());
            }

            dataBuilder.withWarehouse(warehouseId);
        }

        long baseIndexId = olapTable.getBaseIndexId();
        Map<Integer, Column> columnUniqueIdToColumn = Maps.newHashMap();
        // begin checking each table
        // ATTN: DO NOT change any meta in this loop
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
                if (modifyFieldColumns.contains(alterColumn.getName())) {
                    continue;
                }
                Optional<Column> col = originSchema.stream().filter(c -> c.nameEquals(alterColumn.getName(), true)).findFirst();
                if (col.isPresent() && !alterColumn.equals(col.get())) {
                    col.get().checkSchemaChangeAllowed(alterColumn);
                }
            }

            // 3. check partition key
            checkPartitionColumnChange(olapTable, alterSchema, alterIndexId);

            // 4. check distribution key:
            checkDistributionColumnChange(olapTable, alterSchema, alterIndexId);

            // 5. calc short key
            List<Integer> sortKeyIdxes = new ArrayList<>();
            List<Integer> sortKeyUniqueIds = new ArrayList<>();
            MaterializedIndexMeta index = olapTable.getIndexMetaByIndexId(alterIndexId);
            // if sortKeyUniqueIds is empty, the table maybe create in old version and we should use sortKeyIdxes
            // to determine which columns are sort key columns
            boolean useSortKeyUniqueId = (index.getSortKeyUniqueIds() != null) &&
                    (!index.getSortKeyUniqueIds().isEmpty());
            if (index.getSortKeyIdxes() != null && baseIndexId == alterIndexId) {
                List<Integer> originSortKeyIdxes = index.getSortKeyIdxes();
                for (Integer colIdx : originSortKeyIdxes) {
                    String columnName = index.getSchema().get(colIdx).getName();
                    Optional<Column> oneCol =
                            alterSchema.stream().filter(c -> c.nameEquals(columnName, true)).findFirst();
                    if (oneCol.isEmpty()) {
                        LOG.warn("Sort Key Column[" + columnName + "] not exists in new schema");
                        throw new DdlException("Sort Key Column[" + columnName + "] not exists in new schema");
                    }
                    int sortKeyIdx = alterSchema.indexOf(oneCol.get());
                    sortKeyIdxes.add(sortKeyIdx);
                    if (useSortKeyUniqueId) {
                        sortKeyUniqueIds.add(alterSchema.get(sortKeyIdx).getUniqueId());
                    }
                }
            }
            if (!sortKeyIdxes.isEmpty()) {
                short newShortKeyCount = GlobalStateMgr.calcShortKeyColumnCount(alterSchema,
                        indexIdToProperties.get(alterIndexId),
                        sortKeyIdxes);
                LOG.debug("alter index[{}] short key column count: {}", alterIndexId, newShortKeyCount);
                dataBuilder.withNewIndexShortKeyCount(alterIndexId,
                        newShortKeyCount).withNewIndexSchema(alterIndexId, alterSchema);
                dataBuilder.withSortKeyIdxes(sortKeyIdxes);
                dataBuilder.withSortKeyUniqueIds(sortKeyUniqueIds);
            } else {
                short newShortKeyCount = GlobalStateMgr.calcShortKeyColumnCount(alterSchema,
                        indexIdToProperties.get(alterIndexId));
                LOG.debug("alter index[{}] short key column count: {}", alterIndexId, newShortKeyCount);
                dataBuilder.withNewIndexShortKeyCount(alterIndexId,
                        newShortKeyCount).withNewIndexSchema(alterIndexId, alterSchema);
            }

            // 6. check the uniqueness of column unique id
            if (olapTable.getMaxColUniqueId() > Column.COLUMN_UNIQUE_ID_INIT_VALUE) {
                for (Column alterColumn : alterSchema) {
                    Column existedColumn = columnUniqueIdToColumn.putIfAbsent(alterColumn.getUniqueId(), alterColumn);
                    if (existedColumn != null && !existedColumn.getName().equals(alterColumn.getName())) {
                        LOG.warn(
                                "Table {} column {} has same unique id {} with column {}, table max column unique id: {}",
                                olapTable.getName(), alterColumn.getName(), alterColumn.getUniqueId(),
                                existedColumn.getName(), olapTable.getMaxColUniqueId());
                        throw new DdlException("Column " + alterColumn.getName() + " has same unique id "
                                + alterColumn.getUniqueId() + " with column " + existedColumn.getName());
                    }
                }
            }
        } // end for indices

        return dataBuilder.build();
    }

    private void checkPartitionColumnChange(OlapTable olapTable, List<Column> alterSchema, long alterIndexId)
            throws DdlException {
        // Since partition key and distribution key's schema change can only happen in base index,
        // only check it in base index.There may be some trivial changes between base index and other
        // indices(eg: AggregateType), so check it in base index.
        if (alterIndexId != olapTable.getBaseIndexId()) {
            return;
        }

        List<Column> partitionColumns = olapTable.getPartitionInfo().getPartitionColumns(olapTable.getIdToColumn());
        for (Column partitionCol : partitionColumns) {
            String colName = partitionCol.getName();
            Optional<Column> col = alterSchema.stream().filter(c -> c.nameEquals(colName, true)).findFirst();
            if (col.isPresent() && !col.get().equals(partitionCol)) {
                throw new DdlException("Can not modify partition column[" + colName + "]. index["
                        + olapTable.getIndexNameById(alterIndexId) + "]");
            }
            if (col.isEmpty() && alterIndexId == olapTable.getBaseIndexId()) {
                // 2.1 partition column cannot be deleted.
                throw new DdlException("Partition column[" + partitionCol.getName()
                        + "] cannot be dropped. index[" + olapTable.getIndexNameById(alterIndexId) + "]");
                // ATTN. partition columns' order also need remaining unchanged.
                // for now, we only allow one partition column, so no need to check order.
            }
        } // end for partitionColumns
    }

    private void checkDistributionColumnChange(OlapTable olapTable, List<Column> alterSchema, long alterIndexId)
            throws DdlException {
        // Since partition key and distribution key's schema change can only happen in base index,
        // only check it in base index.There may be some trivial changes between base index and other
        // indices(eg: AggregateType), so check it in base index.
        if (alterIndexId != olapTable.getBaseIndexId()) {
            return;
        }

        List<Column> distributionColumns = MetaUtils.getColumnsByColumnIds(
                olapTable, olapTable.getDefaultDistributionInfo().getDistributionColumns());
        for (Column distributionCol : distributionColumns) {
            String colName = distributionCol.getName();
            Optional<Column> col = alterSchema.stream().filter(c -> c.nameEquals(colName, true)).findFirst();
            if (col.isPresent() && !col.get().equals(distributionCol)) {
                throw new DdlException("Can not modify distribution column[" + colName + "]. index["
                        + olapTable.getIndexNameById(alterIndexId) + "]");
            }
            if (col.isEmpty() && alterIndexId == olapTable.getBaseIndexId()) {
                // 2.2 distribution column cannot be deleted.
                throw new DdlException("Distribution column[" + distributionCol.getName()
                        + "] cannot be dropped. index[" + olapTable.getIndexNameById(alterIndexId) + "]");
            }
        } // end for distributionCols
    }

    private AlterJobV2 createJobForProcessModifySortKeyColumn(long dbId, OlapTable olapTable,
                                                              Map<Long, LinkedList<Column>> indexSchemaMap,
                                                              List<Integer> sortKeyIdxes,
                                                              List<Integer> sortKeyUniqueIds) throws UserException {
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
                .withSortKeyIdxes(sortKeyIdxes)
                .withSortKeyUniqueIds(sortKeyUniqueIds);

        if (RunMode.isSharedDataMode()) {
            // check warehouse
            long warehouseId = ConnectContext.get().getCurrentWarehouseId();
            List<Long> computeNodeIs = GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllComputeNodeIds(warehouseId);
            if (computeNodeIs.isEmpty()) {
                Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseId);
                throw new DdlException("no available compute nodes in warehouse " + warehouse.getName());
            }

            jobBuilder.withWarehouse(warehouseId);
        }

        long tableId = olapTable.getId();
        Long alterIndexId = olapTable.getBaseIndexId();
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

    public List<List<Comparable>> getOptimizeJobInfosByDb(Database db) {
        List<List<Comparable>> optimizeJobInfos = new LinkedList<>();
        getAlterJobV2Infos(db, AlterJobV2.JobType.OPTIMIZE, optimizeJobInfos);

        // sort by "JobId", "PartitionName", "CreateTime", "FinishTime", "IndexName", "IndexState"
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1, 2, 3, 4, 5);
        optimizeJobInfos.sort(comparator);
        return optimizeJobInfos;
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        List<List<Comparable>> schemaChangeJobInfos = new LinkedList<>();
        getAlterJobV2Infos(db, AlterJobV2.JobType.SCHEMA_CHANGE, schemaChangeJobInfos);

        // sort by "JobId", "PartitionName", "CreateTime", "FinishTime", "IndexName", "IndexState"
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1, 2, 3, 4, 5);
        schemaChangeJobInfos.sort(comparator);
        return schemaChangeJobInfos;
    }

    private void getAlterJobV2Infos(Database db, AlterJobV2.JobType type, List<AlterJobV2> alterJobsV2,
                                    List<List<Comparable>> schemaChangeJobInfos) {
        ConnectContext ctx = ConnectContext.get();
        for (AlterJobV2 alterJob : alterJobsV2) {
            if (alterJob.getDbId() != db.getId()) {
                continue;
            }
            if (alterJob.getType() != type) {
                continue;
            }
            alterJob.getInfo(schemaChangeJobInfos);
        }
    }

    private void getAlterJobV2Infos(Database db, AlterJobV2.JobType type, List<List<Comparable>> schemaChangeJobInfos) {
        getAlterJobV2Infos(db, type, ImmutableList.copyOf(alterJobsV2.values()), schemaChangeJobInfos);
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
        if (olapTable == null) {
            throw new DdlException("olapTable is null");
        }
        boolean fastSchemaEvolution = olapTable.getUseFastSchemaEvolution();
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
        Set<String> modifyFieldColumns = new HashSet<>();
        for (AlterClause alterClause : alterClauses) {
            if (alterClause instanceof AlterTableColumnClause) {
                if (!propertyMap.isEmpty()) {
                    throw new DdlException("reduplicated PROPERTIES");
                }
                propertyMap.putAll(((AlterTableColumnClause) alterClause).getProperties());
            } else if (alterClause instanceof ModifyTablePropertiesClause) {
                if (!propertyMap.isEmpty()) {
                    throw new DdlException("reduplicated PROPERTIES");
                }
                propertyMap.putAll(((ModifyTablePropertiesClause) alterClause).getProperties());
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
                fastSchemaEvolution &=
                        processAddColumn((AddColumnClause) alterClause, olapTable, indexSchemaMap, colUniqueIdSupplier);
            } else if (alterClause instanceof AddColumnsClause) {
                // add columns
                fastSchemaEvolution &=
                        processAddColumns((AddColumnsClause) alterClause, olapTable, indexSchemaMap, colUniqueIdSupplier);
            } else if (alterClause instanceof DropColumnClause) {
                DropColumnClause dropColumnClause = (DropColumnClause) alterClause;
                // check relative mvs with the modified column
                Set<String> modifiedColumns = Set.of(dropColumnClause.getColName());
                checkModifiedColumWithMaterializedViews(olapTable, modifiedColumns);

                // drop column and drop indexes on this column
                fastSchemaEvolution &=
                        processDropColumn((DropColumnClause) alterClause, olapTable, indexSchemaMap,
                                newIndexes);
            } else if (alterClause instanceof ModifyColumnClause) {
                ModifyColumnClause modifyColumnClause = (ModifyColumnClause) alterClause;

                // check relative mvs with the modified column
                Set<String> modifiedColumns = Set.of(modifyColumnClause.getColumn().getName());
                checkModifiedColumWithMaterializedViews(olapTable, modifiedColumns);

                // modify column
                fastSchemaEvolution &= processModifyColumn(modifyColumnClause, olapTable, indexSchemaMap);
            } else if (alterClause instanceof AddFieldClause) {
                if (RunMode.isSharedDataMode() && !Config.enable_alter_struct_column) {
                    throw new DdlException("Add field for struct column not support shared-data mode so far");
                }
                if (!fastSchemaEvolution) {
                    throw new DdlException("Add field for struct column require table enable fast schema evolution");
                }
                AddFieldClause addFieldClause = (AddFieldClause) alterClause;
                modifyFieldColumns = Set.of(addFieldClause.getColName());
                checkModifiedColumWithMaterializedViews(olapTable, modifyFieldColumns);
                int id = colUniqueIdSupplier.getAsInt();
                processAddField((AddFieldClause) alterClause, olapTable, indexSchemaMap, id, newIndexes);
            } else if (alterClause instanceof DropFieldClause) {
                if (RunMode.isSharedDataMode() && !Config.enable_alter_struct_column) {
                    throw new DdlException("Drop field for struct column not support shared-data mode so far");
                }
                if (!fastSchemaEvolution) {
                    throw new DdlException("Drop field for struct column require table enable fast schema evolution");
                }
                DropFieldClause dropFieldClause = (DropFieldClause) alterClause;
                modifyFieldColumns = Set.of(dropFieldClause.getColName());
                checkModifiedColumWithMaterializedViews(olapTable, modifyFieldColumns);
                processDropField((DropFieldClause) alterClause, olapTable, indexSchemaMap, newIndexes);
            } else if (alterClause instanceof ReorderColumnsClause) {
                // reorder column
                fastSchemaEvolution = false;
                if (changeSortKeyColumn((ReorderColumnsClause) alterClause, olapTable)) {
                    // do modify sort key column
                    List<Integer> sortKeyIdxes = new ArrayList<>();
                    List<Integer> sortKeyUniqueIds = new ArrayList<>();
                    processModifySortKeyColumn((ReorderColumnsClause) alterClause, olapTable, indexSchemaMap, sortKeyIdxes,
                            sortKeyUniqueIds);
                    return createJobForProcessModifySortKeyColumn(db.getId(), olapTable, indexSchemaMap, sortKeyIdxes,
                            sortKeyUniqueIds);
                } else {
                    processReorderColumn((ReorderColumnsClause) alterClause, olapTable, indexSchemaMap);
                }
            } else if (alterClause instanceof ModifyTablePropertiesClause) {
                // modify table properties
                // do nothing, properties are already in propertyMap
                fastSchemaEvolution = false;
            } else if (alterClause instanceof CreateIndexClause) {
                fastSchemaEvolution = false;
                processAddIndex((CreateIndexClause) alterClause, olapTable, newIndexes);
            } else if (alterClause instanceof DropIndexClause) {
                fastSchemaEvolution = false;
                processDropIndex((DropIndexClause) alterClause, olapTable, newIndexes);
            } else if (alterClause instanceof OptimizeClause) {
                return createOptimizeTableJob((OptimizeClause) alterClause, db, olapTable, propertyMap);
            } else {
                Preconditions.checkState(false);
            }
        } // end for alter clauses

        SchemaChangeData schemaChangeData = finalAnalyze(db, olapTable, indexSchemaMap, propertyMap, newIndexes,
                modifyFieldColumns);

        if (schemaChangeData.getNewIndexSchema().isEmpty() && !schemaChangeData.isHasIndexChanged()) {
            // Nothing changed.
            return null;
        }

        if (!fastSchemaEvolution) {
            return createJob(schemaChangeData);
        } else if (RunMode.isSharedNothingMode()) {
            fastSchemaEvolutionInShareNothingMode(schemaChangeData);
            return null;
        } else {
            return createFastSchemaEvolutionJobInSharedDataMode(schemaChangeData);
        }
    }

    /**
     * Check related synchronous materialized views before modified columns, throw exceptions
     * if modified columns affect the related rollup/synchronous mvs.
     */
    public void checkModifiedColumWithMaterializedViews(OlapTable olapTable,
                                                        Set<String> modifiedColumns) throws DdlException {
        if (modifiedColumns == null || modifiedColumns.isEmpty()) {
            return;
        }

        // If there is synchronized materialized view referring the column, throw exception.
        if (olapTable.getIndexNameToId().size() > 1) {
            Map<Long, MaterializedIndexMeta> metaMap = olapTable.getIndexIdToMeta();
            for (Map.Entry<Long, MaterializedIndexMeta> entry : metaMap.entrySet()) {
                Long id = entry.getKey();
                if (id == olapTable.getBaseIndexId()) {
                    continue;
                }
                MaterializedIndexMeta meta = entry.getValue();
                List<Column> schema = meta.getSchema();
                // ignore agg_keys type because it's like duplicated without agg functions
                boolean hasAggregateFunction = olapTable.getKeysType() != KeysType.AGG_KEYS &&
                        schema.stream().anyMatch(x -> x.isAggregated());
                if (hasAggregateFunction) {
                    for (Column rollupCol : schema) {
                        if (modifiedColumns.contains(rollupCol.getName())) {
                            throw new DdlException(String.format("Can not drop/modify the column %s, " +
                                            "because the column is used in the related rollup %s, " +
                                            "please drop the rollup index first.",
                                    rollupCol.getName(), olapTable.getIndexNameById(meta.getIndexId())));
                        }
                        if (rollupCol.getRefColumns() != null) {
                            for (SlotRef refColumn : rollupCol.getRefColumns()) {
                                if (modifiedColumns.contains(refColumn.getColumnName())) {
                                    throw new DdlException(String.format("Can not drop/modify the column %s, " +
                                                    "because the column is used in the related rollup %s " +
                                                    "with the define expr:%s, please drop the rollup index first.",
                                            rollupCol.getName(), olapTable.getIndexNameById(meta.getIndexId()),
                                            rollupCol.getDefineExpr().toSql()));
                                }
                            }
                        }
                    }
                }
                if (meta.getWhereClause() != null) {
                    Expr whereExpr = meta.getWhereClause();
                    List<SlotRef> whereSlots = new ArrayList<>();
                    whereExpr.collect(SlotRef.class, whereSlots);
                    for (SlotRef refColumn : whereSlots) {
                        if (modifiedColumns.contains(refColumn.getColumnName())) {
                            throw new DdlException(String.format("Can not drop/modify the column %s, " +
                                            "because the column is used in the related rollup %s " +
                                            "with the where expr:%s, please drop the rollup index first.",
                                    refColumn.getColumn().getName(), olapTable.getIndexNameById(meta.getIndexId()),
                                    meta.getWhereClause().toSql()));
                        }
                    }
                }
            }
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

    public AlterJobV2 createAlterMetaJob(AlterClause alterClause, Database db, OlapTable olapTable)
            throws UserException {
        LakeTableAlterMetaJob alterMetaJob;
        if (alterClause instanceof ModifyTablePropertiesClause) {
            Map<String, String> properties = ((ModifyTablePropertiesClause) alterClause).getProperties();
            // update table meta
            // for now enable_persistent_index
            if (properties.size() > 1) {
                throw new DdlException("Only support alter one property in one stmt");
            }

            boolean enablePersistentIndex = false;
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX)) {
                enablePersistentIndex = PropertyAnalyzer.analyzeBooleanProp(properties,
                        PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, false);
                boolean oldEnablePersistentIndex = olapTable.enablePersistentIndex();
                if (oldEnablePersistentIndex == enablePersistentIndex) {
                    LOG.info(String.format("table: %s enable_persistent_index is %s, nothing need to do",
                            olapTable.getName(), enablePersistentIndex));
                    return null;
                }
            } else {
                throw new DdlException("does not support alter " + properties.entrySet().iterator().next().getKey() +
                        " in shared_data mode");
            }

            long timeoutSecond = PropertyAnalyzer.analyzeTimeout(properties, Config.alter_table_timeout_second);
            alterMetaJob = new LakeTableAlterMetaJob(GlobalStateMgr.getCurrentState().getNextId(),
                    db.getId(),
                    olapTable.getId(), olapTable.getName(), timeoutSecond,
                    TTabletMetaType.ENABLE_PERSISTENT_INDEX, enablePersistentIndex);
        } else {
            // shouldn't happen
            throw new DdlException("only support alter enable_persistent_index in shared_data mode");
        }
        return alterMetaJob;
    }

    public ShowResultSet processLakeTableAlterMeta(AlterClause alterClause, Database db, OlapTable olapTable)
            throws UserException {
        AlterJobV2 alterMetaJob = createAlterMetaJob(alterClause, db, olapTable);
        if (alterMetaJob == null) {
            return null;
        }
        // set table state
        olapTable.setState(OlapTableState.SCHEMA_CHANGE);

        // 2. add schemaChangeJob
        addAlterJobV2(alterMetaJob);

        // 3. write edit log
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(alterMetaJob);
        LOG.info("finished to create alter meta job {} of cloud table: {}", alterMetaJob.getJobId(),
                olapTable.getName());
        return null;
    }

    public void updateTableMeta(Database db, String tableName, Map<String, String> properties,
                                TTabletMetaType metaType)
            throws DdlException {
        List<Partition> partitions = Lists.newArrayList();
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), tableName);

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        try {
            partitions.addAll(olapTable.getPartitions());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
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
        } else if (metaType == TTabletMetaType.BUCKET_SIZE) {
            long bucketSize = Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_BUCKET_SIZE));
            if (bucketSize == olapTable.getAutomaticBucketSize()) {
                return;
            }
        } else if (metaType == TTabletMetaType.MUTABLE_BUCKET_NUM) {
            long mutableBucketNum = Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_MUTABLE_BUCKET_NUM));
            if (mutableBucketNum == olapTable.getMutableBucketNum()) {
                return;
            }
        } else if (metaType == TTabletMetaType.ENABLE_LOAD_PROFILE) {
            boolean enableLoadProfile = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_LOAD_PROFILE));
            if (enableLoadProfile == olapTable.enableLoadProfile()) {
                return;
            }
        } else if (metaType == TTabletMetaType.PRIMARY_INDEX_CACHE_EXPIRE_SEC) {
            int primaryIndexCacheExpireSec = Integer.parseInt(properties.get(
                    PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC));
            if (primaryIndexCacheExpireSec == olapTable.primaryIndexCacheExpireSec()) {
                return;
            }
        } else {
            LOG.warn("meta type: {} does not support", metaType);
            return;
        }

        if (metaType == TTabletMetaType.INMEMORY || metaType == TTabletMetaType.ENABLE_PERSISTENT_INDEX) {
            for (Partition partition : partitions) {
                updatePartitionTabletMeta(db, olapTable.getName(), partition.getName(), metaValue, metaType);
            }
        }

        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE);
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().modifyTableMeta(db, olapTable, properties, metaType);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE);
        }
    }

    // return true means that the modification of FEMeta is successful,
    // and as long as the modification of metadata is successful,
    // the final consistency will be achieved through the report handler
    public boolean updateBinlogConfigMeta(Database db, Long tableId, Map<String, String> properties,
                                          TTabletMetaType metaType) {
        List<Partition> partitions = Lists.newArrayList();
        BinlogConfig newBinlogConfig;
        boolean hasChanged = false;
        boolean isModifiedSuccess = true;

        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (olapTable == null) {
            return false;
        }
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        try {
            partitions.addAll(olapTable.getPartitions());
            if (!olapTable.containsBinlogConfig()) {
                newBinlogConfig = new BinlogConfig();
                hasChanged = true;
            } else {
                newBinlogConfig = new BinlogConfig(olapTable.getCurBinlogConfig());
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
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
            long binlogTtl = Long.parseLong(properties.get(
                    PropertyAnalyzer.PROPERTIES_BINLOG_TTL));
            if (binlogTtl != newBinlogConfig.getBinlogTtlSecond()) {
                newBinlogConfig.setBinlogTtlSecond(binlogTtl);
                hasChanged = true;
            }
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE)) {
            long binlogMaxSize = Long.parseLong(properties.get(
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

        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE);
        try {
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

            BinlogConfig oldBinlogConfig = olapTable.getCurBinlogConfig();
            GlobalStateMgr.getCurrentState().getLocalMetastore().modifyBinlogMeta(db, olapTable, newBinlogConfig);
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
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE);
        }

        // TODO optimize by asynchronous rpc
        if (metaType != TTabletMetaType.DISABLE_BINLOG) {
            try {
                for (Partition partition : partitions) {
                    updateBinlogPartitionTabletMeta(db, olapTable.getName(), partition.getName(), olapTable.getCurBinlogConfig(),
                            TTabletMetaType.BINLOG_CONFIG);
                }
            } catch (DdlException e) {
                LOG.warn("Failed to execute updateBinlogPartitionTabletMeta", e);
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
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), tableName);
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        try {
            for (String partitionName : partitionNames) {
                Partition partition = olapTable.getPartition(partitionName);
                if (partition == null) {
                    throw new DdlException("Partition[" + partitionName + "] does not exist in " +
                            "table[" + olapTable.getName() + "]");
                }
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
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
        // be id -> Set<tablet id>
        Map<Long, Set<Long>> beIdToTabletId = Maps.newHashMap();
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), tableName);

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        try {
            Partition partition = olapTable.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException(
                        "Partition[" + partitionName + "] does not exist in table[" + olapTable.getName() + "]");
            }

            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                MaterializedIndex baseIndex = physicalPartition.getBaseIndex();
                for (Tablet tablet : baseIndex.getTablets()) {
                    for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                        Set<Long> tabletSet = beIdToTabletId.computeIfAbsent(replica.getBackendId(), k -> Sets.newHashSet());
                        tabletSet.add(tablet.getId());
                    }
                }
            }

        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        }

        int totalTaskNum = beIdToTabletId.keySet().size();
        MarkedCountDownLatch<Long, Set<Long>> countDownLatch = new MarkedCountDownLatch<>(totalTaskNum);
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Map.Entry<Long, Set<Long>> kv : beIdToTabletId.entrySet()) {
            countDownLatch.addMark(kv.getKey(), kv.getValue());
            TabletMetadataUpdateAgentTask task = TabletMetadataUpdateAgentTaskFactory.createBinlogConfigUpdateTask(
                    kv.getKey(), kv.getValue(), binlogConfig);
            task.setLatch(countDownLatch);
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
                    List<Map.Entry<Long, Set<Long>>> unfinishedMarks = countDownLatch.getLeftMarks();
                    // only show at most 3 results
                    List<Map.Entry<Long, Set<Long>>> subList =
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
        // be id -> <tablet id>
        Map<Long, Set<Long>> beIdToTabletSet = Maps.newHashMap();
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), tableName);

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        try {
            Partition partition = olapTable.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException(
                        "Partition[" + partitionName + "] does not exist in table[" + olapTable.getName() + "]");
            }

            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    for (Tablet tablet : index.getTablets()) {
                        for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                            Set<Long> tabletSet = beIdToTabletSet.computeIfAbsent(replica.getBackendId(), k -> Sets.newHashSet());
                            tabletSet.add(tablet.getId());
                        }
                    }
                }
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        }

        int totalTaskNum = beIdToTabletSet.keySet().size();
        MarkedCountDownLatch<Long, Set<Long>> countDownLatch = new MarkedCountDownLatch<>(totalTaskNum);
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Map.Entry<Long, Set<Long>> kv : beIdToTabletSet.entrySet()) {
            countDownLatch.addMark(kv.getKey(), kv.getValue());
            long backendId = kv.getKey();
            Set<Long> tablets = kv.getValue();
            TabletMetadataUpdateAgentTask task = TabletMetadataUpdateAgentTaskFactory
                    .createGenericBooleanPropertyUpdateTask(backendId, tablets, metaValue, metaType);
            Preconditions.checkState(task != null, "task is null");
            task.setLatch(countDownLatch);
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
                    List<Map.Entry<Long, Set<Long>>> unfinishedMarks = countDownLatch.getLeftMarks();
                    // only show at most 3 results
                    List<Map.Entry<Long, Set<Long>>> subList =
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
        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, LockType.READ)) {
            throw new DdlException(String.format("db:%s does not exists.", db.getFullName()));
        }
        TableProperty tableProperty;
        OlapTable olapTable;
        try {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
            if (table == null) {
                throw new DdlException(String.format("table:%s does not exist", tableName));
            }
            olapTable = (OlapTable) table;
            tableProperty = olapTable.getTableProperty();
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
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
                } catch (SemanticException e) {
                    throw new DdlException(
                            String.format("analyze table unique constraint:%s failed, msg: %s",
                                    properties.get(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT), e.getDetailMsg()), e);
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
                } catch (SemanticException e) {
                    throw new DdlException(
                            String.format("analyze table foreign key constraint:%s failed, msg: %s",
                                    properties.get(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT), e.getDetailMsg()), e);
                }
            }
        }

        if (!hasChanged) {
            return;
        }
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE);
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().modifyTableConstraint(db, tableName, properties);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE);
        }
    }

    @Override
    public void cancel(CancelStmt stmt) throws DdlException {
        cancel(stmt, "user cancelled");
    }

    public void cancel(CancelStmt stmt, String reason) throws DdlException {
        CancelAlterTableStmt cancelAlterTableStmt = (CancelAlterTableStmt) stmt;

        String dbName = cancelAlterTableStmt.getDbName();
        String tableName = cancelAlterTableStmt.getTableName();
        Preconditions.checkState(!Strings.isNullOrEmpty(dbName));
        Preconditions.checkState(!Strings.isNullOrEmpty(tableName));

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database[" + dbName + "] does not exist");
        }

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        if (table == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
        }
        if (!(table instanceof OlapTable)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_OLAP_TABLE, tableName);
        }

        AlterJobV2 schemaChangeJobV2 = null;
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getState() != OlapTableState.SCHEMA_CHANGE
                    && olapTable.getState() != OlapTableState.WAITING_STABLE) {
                throw new DdlException("Table[" + tableName + "] is not under SCHEMA_CHANGE/WAITING_STABLE.");
            }

            // find from new alter jobs first
            List<AlterJobV2> schemaChangeJobV2List = getUnfinishedAlterJobV2ByTableId(olapTable.getId());
            // current schemaChangeJob job doesn't support batch operation,so just need to get one job
            schemaChangeJobV2 =
                    schemaChangeJobV2List.isEmpty() ? null : Iterables.getOnlyElement(schemaChangeJobV2List);
            if (schemaChangeJobV2 == null) {
                throw new DdlException(
                        "Table[" + tableName + "] is under SCHEMA_CHANGE but job does not exits.");
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        }

        // alter job v2's cancel must be called outside the database lock
        if (!schemaChangeJobV2.cancel(reason)) {
            throw new DdlException("Job can not be cancelled. State: " + schemaChangeJobV2.getJobState());
        }
    }

    private void processAddIndex(CreateIndexClause alterClause, OlapTable olapTable, List<Index> newIndexes)
            throws UserException {
        Index newIndex = alterClause.getIndex();
        if (newIndex == null) {
            return;
        }

        if (newIndex.getIndexType() == IndexType.GIN && olapTable.enableReplicatedStorage()) {
            throw new SemanticException("GIN does not support replicated mode");
        }

        List<Index> existedIndexes = olapTable.getIndexes();
        IndexDef indexDef = alterClause.getIndexDef();
        Set<String> newColset = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        newColset.addAll(indexDef.getColumns());
        for (Index existedIdx : existedIndexes) {
            // check the index id only if the index is CompatibleIndex(GIN)
            // Bitmap index/Ngram Bloom filter index's id is always be -1
            if (IndexDef.IndexType.isCompatibleIndex(existedIdx.getIndexType()) &&
                    IndexDef.IndexType.isCompatibleIndex(newIndex.getIndexType()) &&
                    existedIdx.getIndexId() >= newIndex.getIndexId()) {
                throw new IllegalStateException(
                        String.format("New index id %s should be lg than existed idx %s in OlapTable",
                                newIndex.getIndexId(), existedIdx.getIndexId()));
            }
            if (existedIdx.getIndexName().equalsIgnoreCase(indexDef.getIndexName())) {
                throw new DdlException("index `" + indexDef.getIndexName() + "` already exist.");
            }
            Set<String> existedIdxColSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            existedIdxColSet.addAll(MetaUtils.getColumnNamesByColumnIds(olapTable, existedIdx.getColumns()));
            if (newColset.equals(existedIdxColSet)) {
                throw new DdlException(
                        "index for columns (" + String.join(",", indexDef.getColumns()) + " ) already exist.");
            }
        }

        for (String col : indexDef.getColumns()) {
            Column column = olapTable.getColumn(col);
            if (column != null) {
                // only throw DdlException
                try {
                    indexDef.checkColumn(column, olapTable.getKeysType());
                } catch (Exception e) {
                    throw new DdlException(e.getMessage());
                }
            } else {
                throw new DdlException(indexDef.getIndexName() + " column does not exist in table. invalid column: " + col);
            }
        }
        Preconditions.checkArgument(newIndex.isValidIndex(),
                "You should ensure that the indexId of the new type index was assigned when the table is OlapTable");
        newIndexes.add(newIndex);
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

    // the invoker should keep write lock
    // this function will update the table index meta according to the `indexSchemaMap` and the
    // `indexSchemaMap` keep the latest column set for each index.
    // there are two scenarios:
    //   1. add/drop column with fast schema evolution in shared-nothing mode
    //   2. add/drop field for a struct colum with fast schema evolution in shared-nothing mode(modify column actually)
    // e.g:
    //   origin table schema: {c1: int, c2: int, c3: Struct<v1 int, v2 int>}
    //       a. add a new column `c4 int` and the table schema will be set {c1: int, c2: int, c3: Struct<v1 int, v2 int>, c4: int}
    //       b. add a new field `v3 int` into `c3` column and the table schema will be set"
    //          {c1: int, c2: int, c3: Struct<v1 int, v2 int, v3 int>}
    public void modifyTableAddOrDrop(Database db, OlapTable olapTable,
                                     Map<Long, List<Column>> indexSchemaMap,
                                     List<Index> indexes, long jobId, long txnId,
                                     Map<Long, Long> indexToNewSchemaId, boolean isReplay)
            throws DdlException, NotImplementedException {
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE);
        try {
            LOG.debug("indexSchemaMap:{}, indexes:{}", indexSchemaMap, indexes);
            if (olapTable.getState() == OlapTableState.ROLLUP) {
                throw new DdlException("Table[" + olapTable.getName() + "] is doing ROLLUP job");
            }

            // for now table's state can only be NORMAL
            Preconditions.checkState(olapTable.getState() == OlapTableState.NORMAL, olapTable.getState().name());
            olapTable.setState(OlapTableState.UPDATING_META);
            SchemaChangeJobV2 schemaChangeJob = new SchemaChangeJobV2(jobId, db.getId(), olapTable.getId(),
                    olapTable.getName(), 1000);
            // update base index schema
            Set<String> modifiedColumns = Sets.newHashSet();
            boolean hasMv = !olapTable.getRelatedMaterializedViews().isEmpty();
            for (Map.Entry<Long, List<Column>> entry : indexSchemaMap.entrySet()) {
                Long idx = entry.getKey();
                List<Column> indexSchema = entry.getValue();
                // modify the copied indexMeta and put the update result in the indexIdToMeta
                MaterializedIndexMeta currentIndexMeta = olapTable.getIndexMetaByIndexId(idx).shallowCopy();
                List<Column> originSchema = currentIndexMeta.getSchema();

                if (hasMv) {
                    modifiedColumns.addAll(AlterHelper.collectDroppedOrModifiedColumns(originSchema, indexSchema));
                }

                List<Integer> sortKeyUniqueIds = currentIndexMeta.getSortKeyUniqueIds();
                List<Integer> newSortKeyIdxes = new ArrayList<>();
                if (sortKeyUniqueIds != null) {
                    for (Integer uniqueId : sortKeyUniqueIds) {
                        Optional<Column> col = indexSchema.stream().filter(c -> c.getUniqueId() == uniqueId).findFirst();
                        if (col.isEmpty()) {
                            throw new DdlException("Sork key col with unique id: " + uniqueId + " not exists");
                        }
                        int sortKeyIdx = indexSchema.indexOf(col.get());
                        newSortKeyIdxes.add(sortKeyIdx);
                    }
                }

                currentIndexMeta.setSchema(indexSchema);
                if (!newSortKeyIdxes.isEmpty()) {
                    currentIndexMeta.setSortKeyIdxes(newSortKeyIdxes);
                }

                int currentSchemaVersion = currentIndexMeta.getSchemaVersion();
                int newSchemaVersion = currentSchemaVersion + 1;
                currentIndexMeta.setSchemaVersion(newSchemaVersion);
                // update the indexIdToMeta
                olapTable.getIndexIdToMeta().put(idx, currentIndexMeta);
                // if FE upgrade from old version and replay journal, the indexToNewSchemaId maybe null
                if (indexToNewSchemaId != null) {
                    currentIndexMeta.setSchemaId(indexToNewSchemaId.get(idx));
                }

                schemaChangeJob.addIndexSchema(idx, idx, olapTable.getIndexNameById(idx), newSchemaVersion,
                        currentIndexMeta.getSchemaHash(), currentIndexMeta.getShortKeyColumnCount(),
                        indexSchema);
            }
            olapTable.setIndexes(indexes);
            olapTable.rebuildFullSchema();

            // If modified columns are already done, inactive related mv
            inactiveRelatedMaterializedViews(db, olapTable, modifiedColumns);

            if (!isReplay) {
                TableAddOrDropColumnsInfo info = new TableAddOrDropColumnsInfo(db.getId(), olapTable.getId(),
                        indexSchemaMap, indexes, jobId, txnId, indexToNewSchemaId);
                LOG.debug("logModifyTableAddOrDrop info:{}", info);
                GlobalStateMgr.getCurrentState().getEditLog().logModifyTableAddOrDrop(info);
            }

            schemaChangeJob.setWatershedTxnId(txnId);
            schemaChangeJob.setJobState(AlterJobV2.JobState.FINISHED);
            schemaChangeJob.setFinishedTimeMs(System.currentTimeMillis());
            this.addAlterJobV2(schemaChangeJob);

            olapTable.lastSchemaUpdateTime.set(System.nanoTime());
            LOG.info("finished modify table's add or drop column(field). table: {}, is replay: {}", olapTable.getName(),
                    isReplay);
        } finally {
            olapTable.setState(OlapTableState.NORMAL);
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE);
        }
    }

    public void replayModifyTableAddOrDrop(TableAddOrDropColumnsInfo info) throws
            MetaNotFoundException {
        LOG.debug("info:{}", info);
        long dbId = info.getDbId();
        long tableId = info.getTableId();
        Map<Long, List<Column>> indexSchemaMap = info.getIndexSchemaMap();
        Map<Long, Long> indexToNewSchemaId = info.getIndexToNewSchemaId();
        List<Index> indexes = info.getIndexes();
        long jobId = info.getJobId();

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        Preconditions.checkArgument(table instanceof OlapTable,
                "Target of light schema change must be olap table");
        OlapTable olapTable = (OlapTable) table;
        Locker locker = new Locker();
        try {
            locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE);
            modifyTableAddOrDrop(db, olapTable, indexSchemaMap, indexes, jobId, info.getTxnId(),
                    indexToNewSchemaId, true);
        } catch (DdlException e) {
            // should not happen
            LOG.warn("failed to replay modify table add or drop columns", e);
        } catch (NotImplementedException e) {
            LOG.error("InternalError", e);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE);
        }
    }

    private void fastSchemaEvolutionInShareNothingMode(SchemaChangeData schemaChangeData)
            throws DdlException, NotImplementedException {
        Preconditions.checkState(RunMode.isSharedNothingMode());
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        long jobId = globalStateMgr.getNextId();
        long txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getTransactionIDGenerator().getNextTransactionId();
        // for schema change add/drop value column optimize, direct modify table meta.
        // when modify this, please also pay attention to the OlapTable#copyOnlyForQuery() operation.
        // try to copy first before modifying, avoiding in-place changes.
        Map<Long, Long> indexToNewSchemaId = new HashMap<Long, Long>();
        for (Long alterIndexId : schemaChangeData.getNewIndexSchema().keySet()) {
            indexToNewSchemaId.put(alterIndexId, globalStateMgr.getNextId());
        }
        // for schema change add/drop value column optimize, direct modify table meta.
        // when modify this, please also pay attention to the OlapTable#copyOnlyForQuery() operation.
        // try to copy first before modifying, avoiding in-place changes.
        modifyTableAddOrDrop(schemaChangeData.getDatabase(), schemaChangeData.getTable(),
                schemaChangeData.getNewIndexSchema(), schemaChangeData.getIndexes(), jobId, txnId,
                indexToNewSchemaId, false);
    }

    private AlterJobV2 createFastSchemaEvolutionJobInSharedDataMode(SchemaChangeData schemaChangeData) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        long jobId = globalStateMgr.getNextId();
        long dbId = schemaChangeData.getDatabase().getId();
        long tableId = schemaChangeData.getTable().getId();
        long timeoutMs = schemaChangeData.getTimeoutInSeconds() * 1000L;
        String tableName = schemaChangeData.getTable().getName();
        LakeTableAsyncFastSchemaChangeJob job = new LakeTableAsyncFastSchemaChangeJob(jobId, dbId, tableId, tableName, timeoutMs);
        for (Map.Entry<Long, List<Column>> entry : schemaChangeData.getNewIndexSchema().entrySet()) {
            long indexId = entry.getKey();
            String indexName = schemaChangeData.getTable().getIndexNameById(indexId);
            int schemaVersion = 1 + schemaChangeData.getTable().getIndexMetaByIndexId(indexId).getSchemaVersion();
            SchemaInfo schemaInfo = SchemaInfo.newBuilder()
                    .setId(globalStateMgr.getNextId())
                    .setKeysType(schemaChangeData.getTable().getKeysType())
                    .setShortKeyColumnCount(schemaChangeData.getNewIndexShortKeyCount().get(indexId))
                    .setStorageType(schemaChangeData.getTable().getStorageType())
                    .setVersion(schemaVersion)
                    .addColumns(entry.getValue())
                    .setBloomFilterColumnNames(schemaChangeData.getBloomFilterColumns())
                    .setBloomFilterFpp(schemaChangeData.getBloomFilterFpp())
                    .setSortKeyIndexes(schemaChangeData.getSortKeyIdxes())
                    .setSortKeyUniqueIds(schemaChangeData.getSortKeyUniqueIds())
                    .setIndexes(schemaChangeData.getIndexes())
                    .build();
            job.setIndexTabletSchema(indexId, indexName, schemaInfo);
        }
        job.setWarehouseId(schemaChangeData.getWarehouseId());
        return job;
    }

    private AlterJobV2 createJob(@NotNull SchemaChangeData schemaChangeData) throws UserException {
        AlterJobV2Builder jobBuilder = schemaChangeData.getTable().alterTable();
        return jobBuilder.withJobId(GlobalStateMgr.getCurrentState().getNextId())
                .withDbId(schemaChangeData.getDatabase().getId())
                .withTimeoutSeconds(schemaChangeData.getTimeoutInSeconds())
                .withAlterIndexInfo(schemaChangeData.isHasIndexChanged(), schemaChangeData.getIndexes())
                .withStartTime(ConnectContext.get().getStartTime())
                .withBloomFilterColumns(schemaChangeData.getBloomFilterColumns(), schemaChangeData.getBloomFilterFpp())
                .withBloomFilterColumnsChanged(schemaChangeData.isBloomFilterColumnsChanged())
                .withNewIndexShortKeyCount(schemaChangeData.getNewIndexShortKeyCount())
                .withSortKeyIdxes(schemaChangeData.getSortKeyIdxes())
                .withSortKeyUniqueIds(schemaChangeData.getSortKeyUniqueIds())
                .withNewIndexSchema(schemaChangeData.getNewIndexSchema())
                .withWarehouse(schemaChangeData.getWarehouseId())
                .build();
    }
}
