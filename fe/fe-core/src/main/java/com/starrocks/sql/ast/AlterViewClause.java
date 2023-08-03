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

import com.starrocks.alter.AlterOpType;
import com.starrocks.catalog.Column;

import java.util.List;

public class AlterViewClause extends AlterClause {
    protected List<ColWithComment> colWithComments;
    protected QueryStatement queryStatement;

    protected List<Column> columns;
    protected String inlineViewDef;

    public AlterViewClause(List<ColWithComment> colWithComments, QueryStatement queryStatement) {
        super(AlterOpType.ALTER_VIEW);
        this.colWithComments = colWithComments;
        this.queryStatement = queryStatement;
    }

    public List<ColWithComment> getColWithComments() {
        return colWithComments;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    public void setInlineViewDef(String inlineViewDef) {
        this.inlineViewDef = inlineViewDef;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterViewClause(this, context);
    }
}
