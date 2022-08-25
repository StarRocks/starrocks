// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowPartitionsStmt.java

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

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShowPartitionsStmt extends ShowStmt {

    public static final String FILTER_PARTITION_ID = "PartitionId";
    public static final String FILTER_PARTITION_NAME = "PartitionName";
    public static final String FILTER_STATE = "State";
    public static final String FILTER_BUCKETS = "Buckets";
    public static final String FILTER_REPLICATION_NUM = "ReplicationNum";
    public static final String FILTER_LAST_CONSISTENCY_CHECK_TIME = "LastConsistencyCheckTime";

    public static final ImmutableSet<String> FILTER_COLUMNS = ImmutableSet.<String>builder().add(FILTER_PARTITION_ID)
            .add(FILTER_PARTITION_NAME)
            .add(FILTER_STATE)
            .add(FILTER_BUCKETS)
            .add(FILTER_REPLICATION_NUM)
            .add(FILTER_LAST_CONSISTENCY_CHECK_TIME).build();

    private String dbName;
    private final String tableName;
    private final Expr whereClause;
    private final List<OrderByElement> orderByElements;
    private final LimitElement limitElement;
    private boolean isTempPartition;

    private List<OrderByPair> orderByPairs;
    private Map<String, Expr> filterMap;

    private ProcNodeInterface node;

    public ShowPartitionsStmt(TableName tableName, Expr whereClause, List<OrderByElement> orderByElements,
                              LimitElement limitElement, boolean isTempPartition) {
        this.dbName = tableName.getDb();
        this.tableName = tableName.getTbl();
        this.whereClause = whereClause;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
        if (whereClause != null) {
            this.filterMap = new HashMap<>();
        }
        this.isTempPartition = isTempPartition;
    }

    public List<OrderByPair> getOrderByPairs() {
        return orderByPairs;
    }

    public LimitElement getLimitElement() {
        return limitElement;
    }

    public Map<String, Expr> getFilterMap() {
        return filterMap;
    }

    public ProcNodeInterface getNode() {
        return node;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
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

    @Override
    public String toString() {
        return toSql();
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public Expr getWhereClause() {
        return whereClause;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public boolean isTempPartition() {
        return isTempPartition;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setOrderByPairs(List<OrderByPair> orderByPairs) {
        this.orderByPairs = orderByPairs;
    }

    public void setNode(ProcNodeInterface node) {
        this.node = node;
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowPartitionsStmt(this, context);
    }
}
