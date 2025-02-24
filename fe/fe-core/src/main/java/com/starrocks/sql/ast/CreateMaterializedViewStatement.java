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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PartitionType;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.plan.ExecPlan;

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
    private final List<IndexDef> indexDefs;
    private boolean ifNotExists;
    private String comment;
    private RefreshSchemeClause refreshSchemeDesc;

    // partition by clause which may be list or range partition expr.
    private List<Expr> partitionByExprs;
    // partition type of the mv which is deduced by its referred base table.
    private PartitionType partitionType;

    private Map<String, String> properties;
    private QueryStatement queryStatement;
    private DistributionDesc distributionDesc;
    private final int queryStartIndex;
    private final List<String> sortKeys;
    private KeysType keysType = KeysType.DUP_KEYS;
    // view definition of the mv which has been rewritten by AstToSQLBuilder#toSQL
    protected String inlineViewDef;
    // simple view definition of the mv which has been rewritten by AstToSQLBuilder#buildSimple
    private String simpleViewDef;
    // original view definition of the mv query without any rewrite which can be used in text based rewrite.
    private String originalViewDefineSql;
    // current db name when creating mv
    private String originalDBName;
    private List<BaseTableInfo> baseTableInfos;

    // Maintenance information
    ExecPlan maintenancePlan;
    ColumnRefFactory columnRefFactory;

    // Sink table information
    private List<Column> mvColumnItems = Lists.newArrayList();
    private List<Index> mvIndexes = Lists.newArrayList();
    // MV's output columns that are referred by mv's partition expressions
    private List<Column> partitionColumns;
    // Ref base table partition expression referred by mv's partition by expressions
    private List<Expr> partitionRefTableExprs;

    // Materialized view's output columns may be different from defined query's output columns.
    // Record the indexes based on materialized view's column output.
    // eg: create materialized view mv as select col1, col2, col3 from tbl
    //  desc mv             :  col2, col1, col3
    //  queryOutputIndexes  :  1, 0, 2
    // which means 0th of query output column is in 1th mv's output columns, and 1th -> 0th, 2th -> 2th.
    private List<Integer> queryOutputIndices = Lists.newArrayList();
    // Generated partition columns for mv's partition by expressions, partition expression index to generated column.
    private Map<Integer, Column> generatedPartitionCols = Maps.newHashMap();
    private Map<Expr, Expr> partitionByExprToAdjustExprMap = Maps.newHashMap();

    public CreateMaterializedViewStatement(TableName tableName, boolean ifNotExists,
                                           List<ColWithComment> colWithComments,
                                           List<IndexDef> indexDefs,
                                           String comment,
                                           RefreshSchemeClause refreshSchemeDesc,
                                           List<Expr> partitionByExprs,
                                           DistributionDesc distributionDesc, List<String> sortKeys,
                                           Map<String, String> properties,
                                           QueryStatement queryStatement,
                                           int queryStartIndex,
                                           String originalDBName,
                                           NodePosition pos) {
        super(pos);
        this.tableName = tableName;
        this.colWithComments = colWithComments;
        this.indexDefs = indexDefs;
        this.ifNotExists = ifNotExists;
        this.comment = comment;
        this.refreshSchemeDesc = refreshSchemeDesc;
        this.partitionByExprs = partitionByExprs;
        this.distributionDesc = distributionDesc;
        this.sortKeys = sortKeys;
        this.properties = properties;
        this.queryStartIndex = queryStartIndex;
        this.queryStatement = queryStatement;
        this.originalDBName = originalDBName;
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

    public List<IndexDef> getIndexDefs() {
        return indexDefs;
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

    public RefreshSchemeClause getRefreshSchemeDesc() {
        return refreshSchemeDesc;
    }

    public void setRefreshSchemeDesc(RefreshSchemeClause refreshSchemeDesc) {
        this.refreshSchemeDesc = refreshSchemeDesc;
    }

    /**
     * Get partition by expr of the mv
     */
    public List<Expr> getPartitionByExprs() {
        return partitionByExprs;
    }

    public void setPartitionByExprs(List<Expr> partitionByExprs) {
        this.partitionByExprs = partitionByExprs;
    }

    /**
     * Get partition type of the mv
     * @return
     */
    public PartitionType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(PartitionType partitionType) {
        this.partitionType = partitionType;
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

    public List<String> getSortKeys() {
        return sortKeys;
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

    public String getOriginalViewDefineSql() {
        return originalViewDefineSql;
    }

    public void setOriginalViewDefineSql(String originalViewDefineSql) {
        this.originalViewDefineSql = originalViewDefineSql;
    }

    public int getQueryStartIndex() {
        return queryStartIndex;
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

    public List<Index> getMvIndexes() {
        return mvIndexes;
    }

    public void setMvColumnItems(List<Column> mvColumnItems) {
        this.mvColumnItems = mvColumnItems;
    }

    public void setMvIndexes(List<Index> mvIndexes) {
        this.mvIndexes = mvIndexes;
    }

    public List<BaseTableInfo> getBaseTableInfos() {
        return baseTableInfos;
    }

    public void setBaseTableInfos(List<BaseTableInfo> baseTableInfos) {
        this.baseTableInfos = baseTableInfos;
    }

    public List<Column> getPartitionColumns() {
        return partitionColumns;
    }

    public void setPartitionColumns(List<Column> partitionColumns) {
        this.partitionColumns = partitionColumns;
    }

    public List<Expr> getPartitionRefTableExpr() {
        return partitionRefTableExprs;
    }

    public void setPartitionRefTableExpr(List<Expr> partitionRefTableExprs) {
        this.partitionRefTableExprs = partitionRefTableExprs;
    }

    public ExecPlan getMaintenancePlan() {
        return maintenancePlan;
    }

    public ColumnRefFactory getColumnRefFactory() {
        return columnRefFactory;
    }

    public List<Integer> getQueryOutputIndices() {
        return queryOutputIndices;
    }

    public void setQueryOutputIndices(List<Integer> queryOutputIndices) {
        this.queryOutputIndices = queryOutputIndices;
    }

    public void setMaintenancePlan(ExecPlan maintenancePlan, ColumnRefFactory columnRefFactory) {
        this.maintenancePlan = maintenancePlan;
        this.columnRefFactory = columnRefFactory;
    }

    public Map<Integer, Column> getGeneratedPartitionCols() {
        return generatedPartitionCols;
    }

    public Map<Expr, Expr> getPartitionByExprToAdjustExprMap() {
        return partitionByExprToAdjustExprMap;
    }
    public String getOriginalDBName() {
        return originalDBName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMaterializedViewStatement(this, context);
    }
}
