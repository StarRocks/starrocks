// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/DescribeStmt.java

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

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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

    // empty col num equals to DESC_OLAP_TABLE_ALL_META_DATA.size()
    public static final List<String> EMPTY_ROW = initEmptyRow();

    private TableName dbTableName;
    private ProcNodeInterface node;

    List<List<String>> totalRows;

    private boolean isAllTables;
    private boolean isOlapTable;
    private boolean isMaterializedView;

    public DescribeStmt(TableName dbTableName, boolean isAllTables) {
        this.dbTableName = dbTableName;
        this.totalRows = new LinkedList<List<String>>();
        this.isAllTables = isAllTables;
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

    public List<List<String>> getResultRows() throws AnalysisException {
        if (isAllTables || isMaterializedView) {
            return totalRows;
        } else {
            Preconditions.checkNotNull(node);
            return node.fetchResult().getRows();
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
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
            } else {
                return DESC_MYSQL_TABLE_ALL_META_DATA;
            }
        }
    }

    @Override
    public String toSql() {
        return "DESCRIBE `" + dbTableName + "`" + (isAllTables ? " ALL" : "");
    }

    @Override
    public String toString() {
        return toSql();
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

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
