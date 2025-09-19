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

import com.google.common.base.Preconditions;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DescribeStmt extends ShowStmt {

    private static final List<String> DESC_OLAP_TABLE_ALL_META_DATA = Arrays.asList(
            "IndexName",
            "IndexKeysType",
            "Field",
            "Type",
            "Null",
            "Key",
            "Default",
            "Extra"
    );

    // empty col num equals to DESC_OLAP_TABLE_ALL_META_DATA.size()
    public static final List<String> EMPTY_ROW = initEmptyRow();

    private final TableName dbTableName;
    private ProcNodeInterface node;

    List<List<String>> totalRows;

    private boolean isAllTables;
    private boolean isOlapTable;
    private boolean isMaterializedView;

    private boolean isTableFunctionTable = false;
    private Map<String, String> tableFunctionProperties = null;

    public DescribeStmt(TableName dbTableName, boolean isAllTables) {
        this(dbTableName, isAllTables, NodePosition.ZERO);
    }

    public DescribeStmt(TableName dbTableName, boolean isAllTables, NodePosition pos) {
        super(pos);
        this.dbTableName = dbTableName;
        this.totalRows = new LinkedList<>();
        this.isAllTables = isAllTables;
    }

    public DescribeStmt(Map<String, String> tableFunctionProperties, NodePosition pos) {
        super(pos);
        this.dbTableName = null;
        this.totalRows = new LinkedList<>();
        this.isTableFunctionTable = true;
        this.tableFunctionProperties = tableFunctionProperties;
    }

    public boolean isAllTables() {
        return isAllTables;
    }

    public String getTableName() {
        return dbTableName.getTbl();
    }

    public String getDb() {
        return dbTableName.getDb();
    }

    public TableName getDbTableName() {
        return dbTableName;
    }

    public List<List<String>> getTotalRows() {
        return totalRows;
    }

    public boolean isMaterializedView() {
        return isMaterializedView;
    }

    public void setMaterializedView(boolean materializedView) {
        isMaterializedView = materializedView;
    }

    public void setAllTables(boolean allTables) {
        isAllTables = allTables;
    }

    public ProcNodeInterface getNode() {
        return node;
    }

    public void setNode(ProcNodeInterface node) {
        this.node = node;
    }

    public boolean isOlapTable() {
        return isOlapTable;
    }

    public void setOlapTable(boolean olapTable) {
        isOlapTable = olapTable;
    }

    public boolean isTableFunctionTable() {
        return isTableFunctionTable;
    }

    public Map<String, String> getTableFunctionProperties() {
        return tableFunctionProperties;
    }

    public List<List<String>> getResultRows() throws AnalysisException {
        if (isAllTables || isMaterializedView || isTableFunctionTable) {
            return totalRows;
        } else {
            Preconditions.checkNotNull(node);
            return node.fetchResult().getRows();
        }
    }

    private static List<String> initEmptyRow() {
        List<String> emptyRow = new ArrayList<>(DESC_OLAP_TABLE_ALL_META_DATA.size());
        for (int i = 0; i < DESC_OLAP_TABLE_ALL_META_DATA.size(); i++) {
            emptyRow.add("");
        }
        return emptyRow;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitDescTableStmt(this, context);
    }
}
