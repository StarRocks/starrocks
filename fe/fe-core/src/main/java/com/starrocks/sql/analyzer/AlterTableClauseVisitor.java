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

package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.ColumnPosition;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.WriteQuorum;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.CompactionClause;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropRollupClause;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyPartitionClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.ReorderColumnsClause;
import com.starrocks.sql.ast.ReplacePartitionClause;
import com.starrocks.sql.ast.RollupRenameClause;
import com.starrocks.sql.ast.TableRenameClause;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class AlterTableClauseVisitor extends AstVisitor<Void, ConnectContext> {

    private Table table;

    public void setTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    public void analyze(AlterClause statement, ConnectContext session) {
        visit(statement, session);
    }

    @Override
    public Void visitCreateIndexClause(CreateIndexClause clause, ConnectContext context) {
        IndexDef indexDef = clause.getIndexDef();
        indexDef.analyze();
        clause.setIndex(new Index(indexDef.getIndexName(), indexDef.getColumns(),
                indexDef.getIndexType(), indexDef.getComment()));
        return null;
    }

    @Override
    public Void visitTableRenameClause(TableRenameClause clause, ConnectContext context) {
        String newTableName = clause.getNewTableName();
        if (Strings.isNullOrEmpty(newTableName)) {
            throw new SemanticException("New Table name is not set");
        }
        FeNameFormat.checkTableName(newTableName);
        return null;
    }

    @Override
    public Void visitModifyTablePropertiesClause(ModifyTablePropertiesClause clause, ConnectContext context) {
        Map<String, String> properties = clause.getProperties();
        if (properties.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Properties is not set");
        }

        if (properties.size() != 1
                && !(TableProperty.isSamePrefixProperties(properties, TableProperty.DYNAMIC_PARTITION_PROPERTY_PREFIX)
                || TableProperty.isSamePrefixProperties(properties, TableProperty.BINLOG_PROPERTY_PREFIX))) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Can only set one table property at a time");
        }

        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)) {
            clause.setNeedTableStable(false);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE).equalsIgnoreCase("column")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Can only change storage type to COLUMN");
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE).equalsIgnoreCase("hash")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Can only change distribution type to HASH");
            }
            clause.setNeedTableStable(false);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK).equalsIgnoreCase("true")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK + " should be set to true");
            }
            clause.setNeedTableStable(false);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_BF_FPP)) {
            // do nothing, these 2 properties will be analyzed when creating alter job
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM)) {
            if (WriteQuorum.findTWriteQuorumByName(properties.get(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM)) == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_WRITE_QUORUM + " not valid");
            }
            clause.setNeedTableStable(false);
            clause.setOpType(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC);
        } else if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(properties)) {
            // do nothing, dynamic properties will be analyzed in SchemaChangeHandler.process
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER)) {
            try {
                PropertyAnalyzer.analyzePartitionLiveNumber(properties, false);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
            try {
                PropertyAnalyzer.analyzeReplicationNum(properties, false);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
            }
        } else if (properties.containsKey("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
            short defaultReplicationNum = 0;
            try {
                defaultReplicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, true);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
            }
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Short.toString(defaultReplicationNum));
        } else if (properties.containsKey("default." + PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
            String storageMedium = properties.remove("default." + PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
            properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, storageMedium);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
            clause.setNeedTableStable(false);
            clause.setOpType(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX).equalsIgnoreCase("true") &&
                    !properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX).equalsIgnoreCase("false")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX +
                                " must be bool type(false/true)");
            }
            clause.setNeedTableStable(false);
            clause.setOpType(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE).equalsIgnoreCase("true") &&
                    !properties.get(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE).equalsIgnoreCase("false")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE +
                                " must be bool type(false/true)");
            }
            clause.setNeedTableStable(false);
            clause.setOpType(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE)) {

            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE)) {
                String binlogEnable = properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE);
                if (!(binlogEnable.equals("false") || binlogEnable.equals("true"))) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Property " + PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE +
                                    " must be true of false");
                }
                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL)) {
                    try {
                        Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_TTL));
                    } catch (NumberFormatException e) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                "Property " + PropertyAnalyzer.PROPERTIES_BINLOG_TTL +
                                        " must be long");
                    }
                }
                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE)) {
                    try {
                        Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE));
                    } catch (NumberFormatException e) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                "Property " + PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE +
                                        " must be long");
                    }
                }
            }
            clause.setNeedTableStable(false);
            clause.setOpType(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TABLET_TYPE)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Alter tablet type not supported");
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)) {
            clause.setNeedTableStable(false);
            clause.setOpType(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC);
        } else {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Unknown properties: " + properties);
        }
        return null;
    }

    @Override
    public Void visitModifyPartitionClause(ModifyPartitionClause clause, ConnectContext context) {
        final List<String> partitionNames = clause.getPartitionNames();
        final boolean needExpand = clause.isNeedExpand();
        final Map<String, String> properties = clause.getProperties();
        if (partitionNames == null || (!needExpand && partitionNames.isEmpty())) {
            throw new SemanticException("Partition names is not set or empty");
        }

        if (partitionNames.stream().anyMatch(Strings::isNullOrEmpty)) {
            throw new SemanticException("there are empty partition name");
        }

        if (properties == null || properties.isEmpty()) {
            throw new SemanticException("Properties is not set");
        }

        // check properties here
        try {
            checkProperties(Maps.newHashMap(properties));
        } catch (AnalysisException e) {
            throw new SemanticException("check properties error: %s", e.getMessage());
        }
        return null;
    }

    @Override
    public Void visitAddColumnClause(AddColumnClause clause, ConnectContext context) {
        ColumnDef columnDef = clause.getColumnDef();
        if (columnDef == null) {
            throw new SemanticException("No column definition in add column clause.");
        }
        try {
            columnDef.analyze(true);
        } catch (AnalysisException e) {
            throw new SemanticException(PARSER_ERROR_MSG.invalidColumnDef(e.getMessage()), columnDef.getPos());
        }

        if (columnDef.getType().isTime()) {
            throw new SemanticException("Unsupported data type: TIME");
        }

        if (columnDef.isMaterializedColumn()) {
            if (!table.isOlapTable()) {
                throw new SemanticException("Materialized Column only support olap table");
            }

            if (table.isCloudNativeTable()) {
                throw new SemanticException("Lake table does not support generated column");
            }

            if (((OlapTable) table).getKeysType() == KeysType.AGG_KEYS) {
                throw new SemanticException("Generated Column does not support AGG table");
            }

            clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));

            Expr expr = columnDef.materializedColumnExpr();
            TableName tableName = new TableName(context.getDatabase(), table.getName());

            ExpressionAnalyzer.analyzeExpression(expr, new AnalyzeState(), new Scope(RelationId.anonymous(),
                    new RelationFields(table.getBaseSchema().stream().map(col -> new Field(col.getName(), col.getType(),
                                    tableName, null))
                            .collect(Collectors.toList()))), context);

            // check if contain aggregation
            List<FunctionCallExpr> funcs = Lists.newArrayList();
            expr.collect(FunctionCallExpr.class, funcs);
            for (FunctionCallExpr fn : funcs) {
                if (fn.isAggregateFunction()) {
                    throw new SemanticException("Materialized Column don't support aggregation function");
                }
            }

            // check if the expression refers to other materialized columns
            List<SlotRef> slots = Lists.newArrayList();
            expr.collect(SlotRef.class, slots);
            if (slots.size() != 0) {
                for (SlotRef slot : slots) {
                    Column refColumn = table.getColumn(slot.getColumnName());
                    if (refColumn.isMaterializedColumn()) {
                        throw new SemanticException("Expression can not refers to other materialized columns");
                    }
                    if (refColumn.isAutoIncrement()) {
                        throw new SemanticException("Expression can not refers to AUTO_INCREMENT columns");
                    }
                }
            }

            if (!columnDef.getType().matchesType(expr.getType())) {
                throw new SemanticException("Illege expression type for Materialized Column " +
                        "Column Type: " + columnDef.getType().toString() +
                        ", Expression Type: " + expr.getType().toString());
            }
            clause.setColumn(columnDef.toColumn());
            return null;
        }

        ColumnPosition colPos = clause.getColPos();
        if (colPos == null && table instanceof OlapTable && ((OlapTable) table).hasMaterializedColumn()) {
            List<Column> baseSchema = ((OlapTable) table).getBaseSchema();
            if (baseSchema.size() > 1) {
                for (int columnIdx = 0; columnIdx < baseSchema.size() - 1; ++columnIdx) {
                    if (!baseSchema.get(columnIdx).isMaterializedColumn() &&
                            baseSchema.get(columnIdx + 1).isMaterializedColumn()) {
                        clause.setColPos(new ColumnPosition(baseSchema.get(columnIdx).getName()));
                        break;
                    }
                }
            }
        }
        if (colPos != null) {
            try {
                colPos.analyze();
            } catch (AnalysisException e) {
                throw new SemanticException(PARSER_ERROR_MSG.invalidColumnPos(e.getMessage()), colPos.getPos());
            }
        }
        if (colPos != null && table instanceof OlapTable && colPos.getLastCol() != null) {
            Column afterColumn = table.getColumn(colPos.getLastCol());
            if (afterColumn.isMaterializedColumn()) {
                throw new SemanticException("Can not add column after Materialized Column");
            }
        }

        if (!columnDef.isAllowNull() && columnDef.defaultValueIsNull()) {
            throw new SemanticException(PARSER_ERROR_MSG.withOutDefaultVal(columnDef.getName()), columnDef.getPos());
        }

        if (columnDef.getAggregateType() != null && colPos != null && colPos.isFirst()) {
            throw new SemanticException("Cannot add value column[" + columnDef.getName() + "] at first",
                    columnDef.getPos());
        }

        // Make sure return null if rollup name is empty.
        clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));

        clause.setColumn(columnDef.toColumn());
        return null;
    }

    @Override
    public Void visitAddColumnsClause(AddColumnsClause clause, ConnectContext context) {
        List<ColumnDef> columnDefs = clause.getColumnDefs();
        if (columnDefs == null || columnDefs.isEmpty()) {
            throw new SemanticException("Columns is empty in add columns clause.");
        }
        boolean hasMaterializedColumn = false;
        boolean hasNormalColumn = false;
        for (ColumnDef colDef : columnDefs) {
            try {
                colDef.analyze(true);
            } catch (AnalysisException e) {
                throw new SemanticException(PARSER_ERROR_MSG.invalidColumnDef(e.getMessage()), colDef.getPos());
            }
            if (!colDef.isAllowNull() && colDef.defaultValueIsNull()) {
                throw new SemanticException(PARSER_ERROR_MSG.withOutDefaultVal(colDef.getName()), colDef.getPos());
            }

            if (colDef.isMaterializedColumn()) {
                hasMaterializedColumn = true;

                if (!table.isOlapTable()) {
                    throw new SemanticException("Materialized Column only support olap table");
                }

                if (table.isCloudNativeTable()) {
                    throw new SemanticException("Lake table does not support generated column");
                }

                if (((OlapTable) table).getKeysType() == KeysType.AGG_KEYS) {
                    throw new SemanticException("Generated Column does not support AGG table");
                }

                Expr expr = colDef.materializedColumnExpr();
                TableName tableName = new TableName(context.getDatabase(), table.getName());

                ExpressionAnalyzer.analyzeExpression(expr, new AnalyzeState(), new Scope(RelationId.anonymous(),
                        new RelationFields(table.getBaseSchema().stream().map(col -> new Field(col.getName(), col.getType(),
                                        tableName, null))
                                .collect(Collectors.toList()))), context);

                // check if contain aggregation
                List<FunctionCallExpr> funcs = Lists.newArrayList();
                expr.collect(FunctionCallExpr.class, funcs);
                for (FunctionCallExpr fn : funcs) {
                    if (fn.isAggregateFunction()) {
                        throw new SemanticException("Materialized Column don't support aggregation function");
                    }
                }

                // check if the expression refers to other materialized columns
                List<SlotRef> slots = Lists.newArrayList();
                expr.collect(SlotRef.class, slots);
                if (slots.size() != 0) {
                    for (SlotRef slot : slots) {
                        Column refColumn = table.getColumn(slot.getColumnName());
                        if (refColumn.isMaterializedColumn()) {
                            throw new SemanticException("Expression can not refers to other materialized columns");
                        }
                        if (refColumn.isAutoIncrement()) {
                            throw new SemanticException("Expression can not refers to AUTO_INCREMENT columns");
                        }
                    }
                }

                if (!colDef.getType().matchesType(expr.getType())) {
                    throw new SemanticException("Illege expression type for Materialized Column " +
                            "Column Type: " + colDef.getType().toString() +
                            ", Expression Type: " + expr.getType().toString());
                }
            } else {
                hasNormalColumn = true;
            }
        }

        if (hasMaterializedColumn && hasNormalColumn) {
            throw new SemanticException("Can not add normal column and Materialized Column in the same time");
        }

        if (hasNormalColumn && table instanceof OlapTable && ((OlapTable) table).hasMaterializedColumn()) {
            List<Column> baseSchema = ((OlapTable) table).getBaseSchema();
            if (baseSchema.size() > 1) {
                for (int columnIdx = 0; columnIdx < baseSchema.size() - 1; ++columnIdx) {
                    if (!baseSchema.get(columnIdx).isMaterializedColumn() &&
                            baseSchema.get(columnIdx + 1).isMaterializedColumn()) {
                        ColumnPosition pos = new ColumnPosition(baseSchema.get(columnIdx).getName());
                        clause.setMaterializedColumnPos(pos);
                        break;
                    }
                }
            }
        }

        // Make sure return null if rollup name is empty.
        clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));

        columnDefs.forEach(columnDef -> clause.addColumn(columnDef.toColumn()));
        return null;
    }

    @Override
    public Void visitDropColumnClause(DropColumnClause clause, ConnectContext context) {
        if (Strings.isNullOrEmpty(clause.getColName())) {
            throw new SemanticException(PARSER_ERROR_MSG.invalidColFormat(clause.getColName()));
        }
        clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));

        for (Column column : table.getFullSchema()) {
            if (column.isMaterializedColumn()) {
                List<SlotRef> slots = column.getMaterializedColumnRef();
                for (SlotRef slot : slots) {
                    if (slot.getColumnName().equals(clause.getColName())) {
                        throw new SemanticException("Column: " + clause.getColName() + " can not be dropped" +
                                ", because expression of Materialized Column: " +
                                column.getName() + " will refer to it");
                    }
                }
            }
        }
        return null;
    }

    @Override
    public Void visitModifyColumnClause(ModifyColumnClause clause, ConnectContext context) {
        ColumnDef columnDef = clause.getColumnDef();
        if (columnDef == null) {
            throw new SemanticException("No column definition in modify column clause.");
        }
        try {
            columnDef.analyze(true);
        } catch (AnalysisException e) {
            throw new SemanticException(PARSER_ERROR_MSG.invalidColumnDef(e.getMessage()), columnDef.getPos());
        }

        if (columnDef.getType().isTime()) {
            throw new SemanticException("Unsupported data type: TIME");
        }

        if (columnDef.isMaterializedColumn()) {
            if (!(table instanceof OlapTable)) {
                throw new SemanticException("Materialized Column only support olap table");
            }

            if (table.isCloudNativeTable()) {
                throw new SemanticException("Lake table does not support generated column");
            }

            if (((OlapTable) table).getKeysType() == KeysType.AGG_KEYS) {
                throw new SemanticException("Generated Column does not support AGG table");
            }

            clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));

            Expr expr = columnDef.materializedColumnExpr();
            TableName tableName = new TableName(context.getDatabase(), table.getName());

            ExpressionAnalyzer.analyzeExpression(expr, new AnalyzeState(), new Scope(RelationId.anonymous(),
                    new RelationFields(table.getBaseSchema().stream().map(col -> new Field(col.getName(), col.getType(),
                                    tableName, null))
                            .collect(Collectors.toList()))), context);

            // check if contain aggregation
            List<FunctionCallExpr> funcs = Lists.newArrayList();
            expr.collect(FunctionCallExpr.class, funcs);
            for (FunctionCallExpr fn : funcs) {
                if (fn.isAggregateFunction()) {
                    throw new SemanticException("Materialized Column don't support aggregation function");
                }
            }

            // check if the expression refers to other materialized columns
            List<SlotRef> slots = Lists.newArrayList();
            expr.collect(SlotRef.class, slots);
            if (slots.size() != 0) {
                for (SlotRef slot : slots) {
                    Column refColumn = table.getColumn(slot.getColumnName());
                    if (refColumn.isMaterializedColumn()) {
                        throw new SemanticException("Expression can not refers to other materialized columns");
                    }
                    if (refColumn.isAutoIncrement()) {
                        throw new SemanticException("Expression can not refers to AUTO_INCREMENT columns");
                    }
                }
            }

            if (!columnDef.getType().matchesType(expr.getType())) {
                throw new SemanticException("Illege expression type for Materialized Column " +
                        "Column Type: " + columnDef.getType().toString() +
                        ", Expression Type: " + expr.getType().toString());
            }
            clause.setColumn(columnDef.toColumn());
            return null;
        }

        ColumnPosition colPos = clause.getColPos();
        if (colPos != null) {
            try {
                colPos.analyze();
            } catch (AnalysisException e) {
                throw new SemanticException(PARSER_ERROR_MSG.invalidColumnPos(e.getMessage()), colPos.getPos());
            }
        }
        if (colPos != null && table instanceof OlapTable && colPos.getLastCol() != null) {
            Column afterColumn = table.getColumn(colPos.getLastCol());
            if (afterColumn.isMaterializedColumn()) {
                throw new SemanticException("Can not modify column after Materialized Column");
            }
        }

        clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));

        clause.setColumn(columnDef.toColumn());
        return null;
    }

    @Override
    public Void visitColumnRenameClause(ColumnRenameClause clause, ConnectContext context) {
        if (Strings.isNullOrEmpty(clause.getColName())) {
            throw new SemanticException("Column name is not set");
        }

        if (Strings.isNullOrEmpty(clause.getNewColName())) {
            throw new SemanticException("New column name is not set");
        }

        FeNameFormat.checkColumnName(clause.getNewColName());
        return null;
    }

    @Override
    public Void visitReorderColumnsClause(ReorderColumnsClause clause, ConnectContext context) {
        List<String> columnsByPos = clause.getColumnsByPos();
        if (columnsByPos == null || columnsByPos.isEmpty()) {
            throw new SemanticException("No column in reorder columns clause.");
        }
        for (String col : columnsByPos) {
            if (Strings.isNullOrEmpty(col)) {
                throw new SemanticException("Empty column in reorder columns.");
            }
        }
        clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));
        return null;
    }

    // Check the following properties' legality before modifying partition.
    // 1. replication_num
    // 2. storage_medium && storage_cooldown_time
    // 3. in_memory
    // 4. tablet type
    private void checkProperties(Map<String, String> properties) throws AnalysisException {
        // 1. data property
        DataProperty newDataProperty = null;
        newDataProperty = PropertyAnalyzer.analyzeDataProperty(properties,
                DataProperty.getInferredDefaultDataProperty(), false);
        Preconditions.checkNotNull(newDataProperty);

        // 2. replication num
        short newReplicationNum;
        newReplicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, RunMode.defaultReplicationNum());
        Preconditions.checkState(newReplicationNum != (short) -1);

        // 3. in memory
        PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);

        // 4. tablet type
        PropertyAnalyzer.analyzeTabletType(properties);
    }

    @Override
    public Void visitReplacePartitionClause(ReplacePartitionClause clause, ConnectContext context) {
        if (clause.getPartitionNames().stream().anyMatch(String::isEmpty)) {
            throw new SemanticException("there are empty partition name", clause.getPartition().getPos());
        }

        if (clause.getTempPartitionNames().stream().anyMatch(String::isEmpty)) {
            throw new SemanticException("there are empty partition name", clause.getTempPartition().getPos());
        }


        if (clause.getPartition().isTemp()) {
            throw new SemanticException("Only support replace partitions with temp partitions",
                    clause.getPartition().getPos());
        }

        if (!clause.getTempPartition().isTemp()) {
            throw new SemanticException("Only support replace partitions with temp partitions",
                    clause.getTempPartition().getPos());
        }

        clause.setStrictRange(PropertyAnalyzer.analyzeBooleanProp(
                clause.getProperties(), PropertyAnalyzer.PROPERTIES_STRICT_RANGE, true));
        clause.setUseTempPartitionName(PropertyAnalyzer.analyzeBooleanProp(
                clause.getProperties(), PropertyAnalyzer.PROPERTIES_USE_TEMP_PARTITION_NAME, false));

        if (clause.getProperties() != null && !clause.getProperties().isEmpty()) {
            throw new SemanticException("Unknown properties: " + clause.getProperties());
        }
        return null;
    }

    @Override
    public Void visitPartitionRenameClause(PartitionRenameClause clause, ConnectContext context) {
        try {
            if (Strings.isNullOrEmpty(clause.getPartitionName())) {
                throw new AnalysisException("Partition name is not set");
            }

            if (Strings.isNullOrEmpty(clause.getNewPartitionName())) {
                throw new AnalysisException("New partition name is not set");
            }
            FeNameFormat.checkPartitionName(clause.getNewPartitionName());
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }
        return null;
    }

    @Override
    public Void visitAddRollupClause(AddRollupClause clause, ConnectContext context) {
        try {
            clause.analyze(null);
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }
        return null;
    }

    @Override
    public Void visitDropRollupClause(DropRollupClause clause, ConnectContext context) {
        try {
            clause.analyze(null);
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }
        return null;
    }

    @Override
    public Void visitRollupRenameClause(RollupRenameClause clause, ConnectContext context) {
        try {
            clause.analyze(null);
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }
        return null;
    }

    @Override
    public Void visitCompactionClause(CompactionClause clause, ConnectContext context) {
        final List<String> partitionNames = clause.getPartitionNames();
        if (partitionNames.stream().anyMatch(Strings::isNullOrEmpty)) {
            throw new SemanticException("there are empty partition name");
        }
        return null;
    }
}
