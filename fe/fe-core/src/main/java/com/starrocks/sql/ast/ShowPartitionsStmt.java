// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.qe.ShowResultSetMetaData;

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
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowPartitionsStatement(this, context);
    }
}
