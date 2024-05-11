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

import com.starrocks.analysis.OrderByElement;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

import java.util.LinkedList;
import java.util.List;

public class ShowDataStmt extends ShowStmt {
    private static final ShowResultSetMetaData SHOW_TABLE_DATA_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Size", ScalarType.createVarchar(30)))
                    .addColumn(new Column("ReplicaCount", ScalarType.createVarchar(20)))
                    .build();

    private static final ShowResultSetMetaData SHOW_INDEX_DATA_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("IndexName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Size", ScalarType.createVarchar(30)))
                    .addColumn(new Column("ReplicaCount", ScalarType.createVarchar(20)))
                    .addColumn(new Column("RowCount", ScalarType.createVarchar(20)))
                    .build();

    private String dbName;
    private final String tableName;
    private List<OrderByElement> orderByElements;
    private List<OrderByPair> orderByPairs;
    private final List<List<String>> totalRows;

    public ShowDataStmt(String dbName, String tableName,  List<OrderByElement> orderByElements) {
        this(dbName, tableName, orderByElements, NodePosition.ZERO);
    }

    public ShowDataStmt(String dbName, String tableName,  List<OrderByElement> orderByElements, NodePosition pos) {
        super(pos);
        this.dbName = dbName;
        this.tableName = tableName;
        this.totalRows = new LinkedList<>();
        this.orderByElements = orderByElements;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean hasTable() {
        return this.tableName != null;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public List<OrderByPair> getOrderByPairs() {
        return orderByPairs;
    }

    public void setOrderByPairs(List<OrderByPair> orderByPairs) {
        this.orderByPairs = orderByPairs;
    }

    public List<List<String>> getResultRows() {
        return totalRows;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        if (tableName != null) {
            return SHOW_INDEX_DATA_META_DATA;
        } else {
            return SHOW_TABLE_DATA_META_DATA;
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowDataStatement(this, context);
    }

    public static int analyzeColumn(String tableName, String columnName) {
        ShowResultSetMetaData metaData = (tableName == null)
                ? SHOW_TABLE_DATA_META_DATA
                : SHOW_INDEX_DATA_META_DATA;
        return metaData.getColumnIdx(columnName);
    }
}

