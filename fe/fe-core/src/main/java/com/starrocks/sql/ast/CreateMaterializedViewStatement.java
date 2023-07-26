// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;

import java.util.List;
import java.util.Map;

/**
 * Materialized view is performed to materialize the results of query.
 * This clause is used to create a new materialized view for specified tables
 * through a specified query stmt.
 * The differences with CreateMaterializedViewStmt:
 * 1. Supports querying materialized view directly and try best to keep the result consistent with querying base tables
 * 2. Supports creating mvs on multi tables
 * 3. partition and distribution desc can be specified for each mv independently.
 * 4. Supports complex computation on columns
 * 5. Supports adding predicate in sql for mvs
 * 6. Supports making mvs on external tables
 */
public class CreateMaterializedViewStatement extends DdlStmt {

    private TableName tableName;
    private final List<ColWithComment> colWithComments;

    private boolean ifNotExists;
    private String comment;
    private RefreshSchemeDesc refreshSchemeDesc;
    private ExpressionPartitionDesc expressionPartitionDesc;
    private Map<String, String> properties;
    private QueryStatement queryStatement;
    private DistributionDesc distributionDesc;

    private KeysType keysType = KeysType.DUP_KEYS;
    protected String inlineViewDef;

    private String simpleViewDef;
    private List<BaseTableInfo> baseTableInfos;

    // for create column in mv
    private List<Column> mvColumnItems = Lists.newArrayList();
    private Column partitionColumn;
    // record expression which related with partition by clause
    private Expr partitionRefTableExpr;

    public CreateMaterializedViewStatement(TableName tableName, boolean ifNotExists,
                                           List<ColWithComment> colWithComments, String comment,
                                           RefreshSchemeDesc refreshSchemeDesc, ExpressionPartitionDesc expressionPartitionDesc,
                                           DistributionDesc distributionDesc, Map<String, String> properties,
                                           QueryStatement queryStatement) {
        this.tableName = tableName;
        this.colWithComments = colWithComments;
        this.ifNotExists = ifNotExists;
        this.comment = comment;
        this.refreshSchemeDesc = refreshSchemeDesc;
        this.expressionPartitionDesc = expressionPartitionDesc;
        this.distributionDesc = distributionDesc;
        this.properties = properties;
        this.queryStatement = queryStatement;
    }

    public TableName getTableName() {
        return tableName;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

    public List<ColWithComment> getColWithComments() {
        return colWithComments;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public RefreshSchemeDesc getRefreshSchemeDesc() {
        return refreshSchemeDesc;
    }

    public void setRefreshSchemeDesc(RefreshSchemeDesc refreshSchemeDesc) {
        this.refreshSchemeDesc = refreshSchemeDesc;
    }

    public ExpressionPartitionDesc getPartitionExpDesc() {
        return expressionPartitionDesc;
    }

    public void setPartitionExpDesc(ExpressionPartitionDesc expressionPartitionDesc) {
        this.expressionPartitionDesc = expressionPartitionDesc;
    }

    public void setKeysType(KeysType keysType) {
        this.keysType = keysType;
    }

    public KeysType getKeysType() {
        return keysType;
    }

    public DistributionDesc getDistributionDesc() {
        return distributionDesc;
    }

    public void setDistributionDesc(DistributionDesc distributionDesc) {
        this.distributionDesc = distributionDesc;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    public void setInlineViewDef(String inlineViewDef) {
        this.inlineViewDef = inlineViewDef;
    }

    public String getSimpleViewDef() {
        return simpleViewDef;
    }

    public void setSimpleViewDef(String simpleViewDef) {
        this.simpleViewDef = simpleViewDef;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public void setQueryStatement(QueryStatement queryStatement) {
        this.queryStatement = queryStatement;
    }

    public List<Column> getMvColumnItems() {
        return mvColumnItems;
    }

    public void setMvColumnItems(List<Column> mvColumnItems) {
        this.mvColumnItems = mvColumnItems;
    }

    public List<BaseTableInfo> getBaseTableInfos() {
        return baseTableInfos;
    }

    public void setBaseTableInfos(List<BaseTableInfo> baseTableInfos) {
        this.baseTableInfos = baseTableInfos;
    }

    public Column getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(Column partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    public Expr getPartitionRefTableExpr() {
        return partitionRefTableExpr;
    }

    public void setPartitionRefTableExpr(Expr partitionRefTableExpr) {
        this.partitionRefTableExpr = partitionRefTableExpr;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMaterializedViewStatement(this, context);
    }
}
