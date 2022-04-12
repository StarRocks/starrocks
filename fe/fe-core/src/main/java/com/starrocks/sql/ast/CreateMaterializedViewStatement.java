// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.MVColumnItem;
import com.starrocks.analysis.PartitionDesc;
import com.starrocks.catalog.*;
import com.starrocks.common.*;

import java.util.ArrayList;
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

    private PartitionDesc partitionDesc;

    private Map<String, String> properties;

    private QueryStatement queryStatement;

    private DistributionDesc distributionDesc;

    private String dbName;

    private KeysType myKeyType = KeysType.DUP_KEYS;


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

    public PartitionDesc getPartitionDesc() {
        return partitionDesc;
    }

    public void setPartitionDesc(PartitionDesc partitionDesc) {
        this.partitionDesc = partitionDesc;
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

    private int beginIndexOfAggregation = -1;
    /**
     * origin stmt: select k1, k2, v1, sum(v2) from base_table group by k1, k2, v1
     * mvColumnItemList: [k1: {name: k1, isKey: true, aggType: null, isAggregationTypeImplicit: false},
     * k2: {name: k2, isKey: true, aggType: null, isAggregationTypeImplicit: false},
     * v1: {name: v1, isKey: true, aggType: null, isAggregationTypeImplicit: false},
     * v2: {name: v2, isKey: false, aggType: sum, isAggregationTypeImplicit: false}]
     * This order of mvColumnItemList is meaningful.
     **/
    private List<MVColumnItem> mvColumnItems = new ArrayList<>();
    //if process is replaying log, isReplay is true, otherwise is false, avoid replay process error report, only in Rollup or MaterializedIndexMeta is true
    private boolean isReplay = false;

    private String baseIndexName;


    public CreateMaterializedViewStatement(String mvName, boolean ifNotExists, String comment,
                                           RefreshSchemeDesc refreshSchemeDesc, PartitionDesc partitionDesc,
                                           Map<String, String> properties, QueryStatement queryStatement) {
        this.mvName = mvName;
        this.ifNotExists = ifNotExists;
        this.comment = comment;
        this.refreshSchemeDesc = refreshSchemeDesc;
        this.partitionDesc = partitionDesc;
        this.properties = properties;
        this.queryStatement = queryStatement;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMaterializedViewStatement(this, context);
    }
}
