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


package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.starrocks.analysis.OutFileClause.PARQUET_COMPRESSION_TYPE_MAP;

/**
 * Insert into is performed to load data from the result of query stmt.
 * <p>
 * syntax:
 * INSERT INTO table_name [partition_info] [col_list] [plan_hints] query_stmt
 * <p>
 * table_name: is the name of target table
 * partition_info: PARTITION (p1,p2)
 * the partition info of target table
 * col_list: (c1,c2)
 * the column list of target table
 * plan_hints: [STREAMING,SHUFFLE_HINT]
 * The streaming plan is used by both streaming and non-streaming insert stmt.
 * The only difference is that non-streaming will record the load info in LoadManager and return label.
 * User can check the load info by show load stmt.
 */
public class InsertStmt extends DmlStmt {
    public static final String STREAMING = "STREAMING";

    private final TableName tblName;
    private PartitionNames targetPartitionNames;
    // parsed from targetPartitionNames.
    // if targetPartitionNames is not set, add all formal partitions' id of the table into it
    private List<Long> targetPartitionIds = Lists.newArrayList();
    private List<String> targetColumnNames;
    private QueryStatement queryStatement;
    private String label = null;

    // set after parse all columns and expr in query statement
    // this result expr in the order of target table's columns
    private final ArrayList<Expr> resultExprs = Lists.newArrayList();

    private final Map<String, Expr> exprByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    private Table targetTable;

    private List<Column> targetColumns = Lists.newArrayList();
    private boolean isOverwrite;
    private long overwriteJobId = -1;

    // The default value of this variable is false, which means that the insert operation created by the user
    // it is not allowed to write data to the materialized view.
    // If this is set to true it means a system refresh operation, which is allowed to write to materialized view.
    private boolean isSystem = false;
    // Since insert overwrite internally reuses the insert statement,
    // this variable can be used to distinguish whether a partition is specified.
    private boolean partitionNotSpecifiedInOverwrite = false;

    /**
     * `true` means that it's created by CTAS statement
     */
    private boolean forCTAS = false;

    // tableFunctionAsTargetTable is true if insert statement is parsed from INSERT INTO FILES(..)
    private final boolean tableFunctionAsTargetTable;
    private final Map<String, String> tableFunctionProperties;

    public InsertStmt(TableName tblName, PartitionNames targetPartitionNames, String label, List<String> cols,
                      QueryStatement queryStatement, boolean isOverwrite) {
        this(tblName, targetPartitionNames, label, cols, queryStatement, isOverwrite, NodePosition.ZERO);
    }

    public InsertStmt(TableName tblName, PartitionNames targetPartitionNames, String label, List<String> cols,
                      QueryStatement queryStatement, boolean isOverwrite, NodePosition pos) {
        super(pos);
        this.tblName = tblName;
        this.targetPartitionNames = targetPartitionNames;
        this.label = label;
        this.queryStatement = queryStatement;
        this.targetColumnNames = cols;
        this.isOverwrite = isOverwrite;
        this.tableFunctionAsTargetTable = false;
        this.tableFunctionProperties = null;
    }

    // Ctor for CreateTableAsSelectStmt
    public InsertStmt(TableName name, QueryStatement queryStatement) {
        // CTAS claus hasn't explicit insert stmt, we use the pos of queryStmt to express the location of insertStmt
        super(queryStatement.getPos());
        this.tblName = name;
        this.targetPartitionNames = null;
        this.targetColumnNames = null;
        this.queryStatement = queryStatement;
        this.forCTAS = true;
        this.tableFunctionAsTargetTable = false;
        this.tableFunctionProperties = null;
    }

    // Ctor for INSERT INTO FILES(...)
    public InsertStmt(Map<String, String> tableFunctionProperties, QueryStatement queryStatement, NodePosition pos) {
        super(pos);
        this.tblName = new TableName("table_function_catalog", "table_function_db", "table_function_table");
        this.targetColumnNames = null;
        this.targetPartitionNames = null;
        this.queryStatement = queryStatement;
        this.tableFunctionAsTargetTable = true;
        this.tableFunctionProperties = tableFunctionProperties;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(Table targetTable) {
        this.targetTable = targetTable;
    }

    public boolean isOverwrite() {
        return isOverwrite;
    }

    public void setOverwrite(boolean overwrite) {
        isOverwrite = overwrite;
    }

    public void setOverwriteJobId(long overwriteJobId) {
        this.overwriteJobId = overwriteJobId;
    }

    public boolean hasOverwriteJob() {
        return overwriteJobId > 0;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public void setQueryStatement(QueryStatement queryStatement) {
        this.queryStatement = queryStatement;
    }

    @Override
    public boolean isExplain() {
        return queryStatement.isExplain();
    }

    @Override
    public ExplainLevel getExplainLevel() {
        return queryStatement.getExplainLevel();
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public boolean isSystem() {
        return isSystem;
    }

    public void setSystem(boolean system) {
        isSystem = system;
    }

    @Override
    public TableName getTableName() {
        return tblName;
    }

    public PartitionNames getTargetPartitionNames() {
        return targetPartitionNames;
    }

    public boolean isSpecifyPartitionNames() {
        return targetPartitionNames != null && !targetPartitionNames.isStaticKeyPartitionInsert();
    }

    public void setTargetColumnNames(List<String> targetColumnNames) {
        this.targetColumnNames = targetColumnNames;
    }

    public List<String> getTargetColumnNames() {
        return targetColumnNames;
    }

    public void setTargetPartitionNames(PartitionNames targetPartitionNames) {
        this.targetPartitionNames = targetPartitionNames;
    }

    public void setTargetPartitionIds(List<Long> targetPartitionIds) {
        this.targetPartitionIds = targetPartitionIds;
    }

    public List<Long> getTargetPartitionIds() {
        return targetPartitionIds;
    }

    public void setTargetColumns(List<Column> targetColumns) {
        this.targetColumns = targetColumns;
    }

    public boolean isSpecifyKeyPartition() {
        return targetTable != null && (targetTable.isHiveTable() || targetTable.isIcebergTable()) &&
                isStaticKeyPartitionInsert();
    }

    public boolean isStaticKeyPartitionInsert() {
        return targetPartitionNames != null && targetPartitionNames.isStaticKeyPartitionInsert();
    }

    public boolean isPartitionNotSpecifiedInOverwrite() {
        return partitionNotSpecifiedInOverwrite;
    }

    public void setPartitionNotSpecifiedInOverwrite(boolean partitionNotSpecifiedInOverwrite) {
        this.partitionNotSpecifiedInOverwrite = partitionNotSpecifiedInOverwrite;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        if (isExplain() && !StatementBase.ExplainLevel.ANALYZE.equals(getExplainLevel())) {
            return RedirectStatus.NO_FORWARD;
        } else {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }
    }

    public boolean isForCTAS() {
        return forCTAS;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitInsertStatement(this, context);
    }

    public boolean useTableFunctionAsTargetTable() {
        return tableFunctionAsTargetTable;
    }

    public Map<String, String> getTableFunctionProperties() {
        return tableFunctionProperties;
    }

    public Table makeTableFunctionTable() {
        checkState(tableFunctionAsTargetTable, "tableFunctionAsTargetTable is false");
        // fetch schema from query
        QueryRelation query = getQueryStatement().getQueryRelation();
        List<Field> allFields = query.getRelationFields().getAllFields();
        List<Column> columns = allFields.stream().filter(Field::isVisible).map(field -> new Column(field.getName(),
                field.getType(), field.isNullable())).collect(Collectors.toList());

        // parse table function properties
        Map<String, String> props = getTableFunctionProperties();
        String single = props.getOrDefault("single", "false");
        if (!single.equalsIgnoreCase("true") && !single.equalsIgnoreCase("false")) {
            throw new SemanticException("got invalid parameter \"single\" = \"%s\", expect a boolean value (true or false).",
                    single);
        }

        boolean writeSingleFile = single.equalsIgnoreCase("true");
        String path = props.get("path");
        String format = props.get("format");
        String partitionBy = props.get("partition_by");
        String compressionType = props.get("compression");

        // validate properties
        if (path == null) {
            throw new SemanticException(
                    "path is a mandatory property. \"path\" = \"s3://path/to/your/location/\"");
        }

        if (format == null) {
            throw new SemanticException("format is a mandatory property. " +
                    "Use \"path\" = \"parquet\" as only parquet format is supported now");
        }

        if (!format.equalsIgnoreCase("parquet")) {
            throw new SemanticException("use \"path\" = \"parquet\", as only parquet format is supported now");
        }

        if (compressionType == null) {
            throw new SemanticException("compression is a mandatory property. " +
                    "Use \"compression\" = \"your_chosen_compression_type\". Supported compression types are" +
                    "(uncompressed, gzip, brotli, zstd, lz4).");
        }

        if (!PARQUET_COMPRESSION_TYPE_MAP.containsKey(compressionType)) {
            throw new SemanticException("compression type " + compressionType + " is not supported. " +
                    "Use any of (uncompressed, gzip, brotli, zstd, lz4).");
        }

        if (writeSingleFile && partitionBy != null) {
            throw new SemanticException("cannot use partition_by and single simultaneously.");
        }

        if (writeSingleFile) {
            return new TableFunctionTable(path, format, compressionType, columns, null, true, props);
        }

        if (partitionBy == null) {
            // prepend `data_` if path ends with forward slash
            if (path.endsWith("/")) {
                path += "data_";
            }
            return new TableFunctionTable(path, format, compressionType, columns, null, false, props);
        }

        // extra validation for using partitionBy
        if (!path.endsWith("/")) {
            throw new SemanticException(
                    "If partition_by is used, path should be a directory ends with forward slash(/).");
        }

        List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());

        // parse and validate partition columns
        List<String> partitionColumnNames = Arrays.asList(partitionBy.split(","));
        partitionColumnNames.replaceAll(String::trim);
        partitionColumnNames = partitionColumnNames.stream().distinct().collect(Collectors.toList());

        List<String> unmatchedPartitionColumnNames = partitionColumnNames.stream().filter(col ->
                !columnNames.contains(col)).collect(Collectors.toList());
        if (!unmatchedPartitionColumnNames.isEmpty()) {
            throw new SemanticException("partition columns expected to be a subset of " + columnNames +
                    ", but got extra columns: " + unmatchedPartitionColumnNames);
        }

        List<Integer> partitionColumnIDs = partitionColumnNames.stream().map(columnNames::indexOf).collect(
                Collectors.toList());

        for (Integer partitionColumnID : partitionColumnIDs) {
            Column partitionColumn = columns.get(partitionColumnID);
            Type type = partitionColumn.getType();
            if (type.isBoolean() || type.isIntegerType() || type.isDateType() || type.isStringType()) {
                continue;
            }
            throw new SemanticException("partition column does not support type of " + type);
        }

        return new TableFunctionTable(path, format, compressionType, columns, partitionColumnIDs, false, props);
    }
}
