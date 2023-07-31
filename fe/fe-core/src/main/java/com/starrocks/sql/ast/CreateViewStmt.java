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

import com.google.common.base.Strings;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.epack.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.sql.parser.NodePosition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateViewStmt extends DdlStmt {
    private final TableName tableName;
    private final List<ColWithComment> colWithComments;
    private final boolean ifNotExists;
    private final boolean replace;
    private final String comment;
    protected QueryStatement queryStatement;

    //Resolved by Analyzer
    protected List<Column> columns;
    private String inlineViewDef;

    //Add by Epack
    private List<WithRowAccessPolicy> withRowAccessPolicies;

    public CreateViewStmt(boolean ifNotExists, boolean replace,
                          TableName tableName, List<ColWithComment> colWithComments,
                          String comment,
                          QueryStatement queryStmt,
                          NodePosition pos) {
        super(pos);
        this.ifNotExists = ifNotExists;
        this.replace = replace;
        this.tableName = tableName;
        this.colWithComments = colWithComments;
        this.comment = Strings.nullToEmpty(comment);
        this.queryStatement = queryStmt;
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

    public List<ColWithComment> getColWithComments() {
        return colWithComments;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public boolean isReplace() {
        return replace;
    }

    public String getComment() {
        return comment;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    public void setInlineViewDef(String inlineViewDef) {
        this.inlineViewDef = inlineViewDef;
    }

    public Map<String, WithColumnMaskingPolicy> getMaskingPolicyContextMap() {
        Map<String, WithColumnMaskingPolicy> maskingPolicyMap = new HashMap<>();
        if (colWithComments != null) {
            for (ColWithComment colWithComment : colWithComments) {
                if (colWithComment.getWithColumnMaskingPolicy() != null) {
                    maskingPolicyMap.put(colWithComment.getColName(), colWithComment.getWithColumnMaskingPolicy());
                }
            }
        }
        return maskingPolicyMap;
    }

    public List<WithRowAccessPolicy> getWithRowAccessPolicies() {
        return withRowAccessPolicies;
    }

    public void setWithRowAccessPolicies(List<WithRowAccessPolicy> withRowAccessPolicies) {
        this.withRowAccessPolicies = withRowAccessPolicies;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateViewStatement(this, context);
    }
}
