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
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DescribeStmt extends ShowStmt {

    private static final ShowResultSetMetaData DESC_OLAP_TABLE_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Key", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Extra", ScalarType.createVarchar(30)))
                    .build();

    private static final ShowResultSetMetaData DESC_OLAP_TABLE_ALL_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("IndexName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("IndexKeysType", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Key", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Extra", ScalarType.createVarchar(30)))
                    .build();

    private static final ShowResultSetMetaData DESC_MYSQL_TABLE_ALL_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Host", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Port", ScalarType.createVarchar(10)))
                    .addColumn(new Column("User", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Password", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Database", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(30)))
                    .build();

    private static final ShowResultSetMetaData DESC_TABLE_FUNCTION_TABLE_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(10)))
                    .build();

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

    public void setMaterializedView(boolean materializedView) {
        isMaterializedView = materializedView;
    }

    public void setAllTables(boolean allTables) {
        isAllTables = allTables;
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

    @Override
    public ShowResultSetMetaData getMetaData() {
        if (isTableFunctionTable) {
            return DESC_TABLE_FUNCTION_TABLE_META_DATA;
        }

        if (!isAllTables) {
            if (isMaterializedView) {
                return DESC_OLAP_TABLE_META_DATA;
            } else {
                ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

                ProcResult result = null;
                try {
                    result = node.fetchResult();
                } catch (AnalysisException e) {
                    return builder.build();
                }

                for (String col : result.getColumnNames()) {
                    builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
                }
                return builder.build();
            }
        } else {
            if (isOlapTable) {
                return DESC_OLAP_TABLE_ALL_META_DATA;
            } else if (isMaterializedView) {
                return DESC_OLAP_TABLE_META_DATA;
            } else {
                return DESC_MYSQL_TABLE_ALL_META_DATA;
            }
        }
    }

    private static List<String> initEmptyRow() {
        List<String> emptyRow = new ArrayList<>(DESC_OLAP_TABLE_ALL_META_DATA.getColumns().size());
        for (int i = 0; i < DESC_OLAP_TABLE_ALL_META_DATA.getColumns().size(); i++) {
            emptyRow.add("");
        }
        return emptyRow;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDescTableStmt(this, context);
    }
}
