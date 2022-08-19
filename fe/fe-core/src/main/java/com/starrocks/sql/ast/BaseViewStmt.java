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

package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.ColWithComment;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;

import java.util.List;

public class BaseViewStmt extends DdlStmt {
    protected final TableName tableName;
    protected final List<ColWithComment> cols;
    protected List<Column> finalCols;
    protected String inlineViewDef;
    protected QueryStatement queryStatement;

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

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseViewStatement(this, context);
    }
}
