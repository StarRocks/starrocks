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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    private final List<String> targetColumnNames;
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
        return targetTable != null && targetTable instanceof IcebergTable && isStaticKeyPartitionInsert();
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
        if (isExplain()) {
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
}
