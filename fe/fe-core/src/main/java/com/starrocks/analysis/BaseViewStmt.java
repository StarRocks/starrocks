// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/BaseViewStmt.java

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
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class BaseViewStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(BaseViewStmt.class);

    protected final TableName tableName;
    protected final List<ColWithComment> cols;
    protected QueryStmt viewDefStmt;

    // Set during analyze
    protected List<Column> finalCols;

    protected String originalViewDef;
    protected String inlineViewDef;

    protected QueryStmt cloneStmt;

    protected QueryStatement queryStatement;

    public BaseViewStmt(TableName tableName, List<ColWithComment> cols, QueryStmt queryStmt) {
        Preconditions.checkNotNull(queryStmt);
        this.tableName = tableName;
        this.cols = cols;
        this.viewDefStmt = queryStmt;
        finalCols = Lists.newArrayList();
    }

    public BaseViewStmt(TableName tableName, List<ColWithComment> cols, QueryStatement queryStmt) {
        Preconditions.checkNotNull(queryStmt);
        this.tableName = tableName;
        this.cols = cols;
        this.queryStatement = queryStmt;
        finalCols = Lists.newArrayList();
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTable() {
        return tableName.getTbl();
    }

    public TableName getTableName() {
        return tableName;
    }

    public List<Column> getColumns() {
        return finalCols;
    }

    public void setFinalCols(List<Column> finalCols) {
        this.finalCols = finalCols;
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

    public List<ColWithComment> getCols() {
        return cols;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        if (viewDefStmt.hasOutFileClause()) {
            throw new AnalysisException("Not support OUTFILE clause in CREATE VIEW statement");
        }
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseViewStatement(this, context);
    }
}
