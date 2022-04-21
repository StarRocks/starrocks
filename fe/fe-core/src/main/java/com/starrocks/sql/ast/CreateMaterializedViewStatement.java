// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import org.spark_project.guava.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * //todo add more comments
 * Materialized view is performed to materialize the results of query.
 * This clause is used to create a new materialized view for specified tables
 * through a specified query stmt.
 * <p>
 * Syntax:
 * CREATE MATERIALIZED VIEW [IF NOT EXISTS] mvName
 * [COMMENT]
 * PARTITION BY Table1.Column1
 * REFRESH ASYNC/SYNC
 * AS query_stmt
 * [PROPERTIES ("key" = "value")]
 */
public class CreateMaterializedViewStatement extends DdlStmt {

    private String mvName;

    private boolean ifNotExists;

    private String comment;

    private RefreshSchemeDesc refreshSchemeDesc;

    private PartitionExpDesc partitionExpDesc;

    private Map<String, String> properties;

    private QueryStatement queryStatement;

    private DistributionDesc distributionDesc;

    private KeysType myKeyType = KeysType.DUP_KEYS;

    private String dbName;

    private List<Column> mvColumnItems = Lists.newArrayList();

    private Map<Column, Expr> columnExprMap = Maps.newHashMap();

    public String getMvName() {
        return mvName;
    }

    public void setMvName(String mvName) {
        this.mvName = mvName;
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

    public PartitionExpDesc getPartitionExpDesc() {
        return partitionExpDesc;
    }

    public void setPartitionExpDesc(PartitionExpDesc partitionExpDesc) {
        this.partitionExpDesc = partitionExpDesc;
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

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public void setQueryStatement(QueryStatement queryStatement) {
        this.queryStatement = queryStatement;
    }

    //if process is replaying log, isReplay is true, otherwise is false, avoid replay process error report, only in Rollup or MaterializedIndexMeta is true
    private boolean isReplay = false;

    public List<Column> getMvColumnItems() {
        return mvColumnItems;
    }

    public void setMvColumnItems(List<Column> mvColumnItems) {
        this.mvColumnItems = mvColumnItems;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setColumnExprMap(Map<Column, Expr> columnExprMap) {
        this.columnExprMap = columnExprMap;
    }

    public Map<Column, Expr> getColumnExprMap() {
        return columnExprMap;
    }

    public CreateMaterializedViewStatement(String mvName, boolean ifNotExists, String comment,
                                           RefreshSchemeDesc refreshSchemeDesc, PartitionExpDesc partitionExpDesc,
                                           DistributionDesc distributionDesc, Map<String, String> properties,
                                           QueryStatement queryStatement) {
        this.mvName = mvName;
        this.ifNotExists = ifNotExists;
        this.comment = comment;
        this.refreshSchemeDesc = refreshSchemeDesc;
        this.partitionExpDesc = partitionExpDesc;
        this.distributionDesc = distributionDesc;
        this.properties = properties;
        this.queryStatement = queryStatement;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMaterializedViewStatement(this, context);
    }




}
