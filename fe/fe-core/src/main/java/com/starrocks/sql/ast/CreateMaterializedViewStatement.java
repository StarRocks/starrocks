// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;

import java.util.List;
import java.util.Map;

/**
 * Materialized view is performed to materialize the results of query.
 * This clause is used to create a new materialized view for specified tables
 * through a specified query stmt.
 * The differences with CreateMaterializedViewStmt:
 * 1. Supports querying materiazlied view directly and try best to keep the result consistent with querying base tables
 * 2. Supports creating mvs on multi tables
 * 3. partition and distribution desc can be specified for each mv independently.
 * 4. Supports complex computation on columns
 * 5. Supports adding predicate in sql for mvs
 * 6. Supports making mvs on external tables
 */
public class CreateMaterializedViewStatement extends DdlStmt {

    private TableName tableName;
    private boolean ifNotExists;
    private String comment;
    private RefreshSchemeDesc refreshSchemeDesc;
    private ExpressionPartitionDesc expressionPartitionDesc;
    private Map<String, String> properties;
    private QueryStatement queryStatement;
    private DistributionDesc distributionDesc;
    private KeysType myKeyType = KeysType.DUP_KEYS;
    protected String inlineViewDef;
    // if process is replaying log, isReplay is true, otherwise is false, avoid replay process error report, only in Rollup or MaterializedIndexMeta is true
    private boolean isReplay = false;
    // for create column in mv
    private List<Column> mvColumnItems = Lists.newArrayList();

    public CreateMaterializedViewStatement(TableName tableName, boolean ifNotExists, String comment,
                                           RefreshSchemeDesc refreshSchemeDesc, ExpressionPartitionDesc expressionPartitionDesc,
                                           DistributionDesc distributionDesc, Map<String, String> properties,
                                           QueryStatement queryStatement) {
        this.tableName = tableName;
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

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMaterializedViewStatement(this, context);
    }
}
