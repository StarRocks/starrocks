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

import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class CTERelation extends Relation {
    private final int cteMouldId;
    private final String name;
    private final QueryStatement cteQueryStatement;
    private boolean resolvedInFromClause;

    public CTERelation(int cteMouldId, String name, List<String> columnOutputNames,
                       QueryStatement cteQueryStatement) {
        this(cteMouldId, name, columnOutputNames, cteQueryStatement, NodePosition.ZERO);
    }

    public CTERelation(int cteMouldId, String name, List<String> columnOutputNames,
                       QueryStatement cteQueryStatement, NodePosition pos) {
        super(pos);
        this.cteMouldId = cteMouldId;
        this.name = name;
        this.explicitColumnNames = columnOutputNames;
        this.cteQueryStatement = cteQueryStatement;
    }

    public int getCteMouldId() {
        return cteMouldId;
    }

    public QueryStatement getCteQueryStatement() {
        return cteQueryStatement;
    }

    public String getName() {
        return name;
    }

    public void setResolvedInFromClause(boolean resolvedInFromClause) {
        this.resolvedInFromClause = resolvedInFromClause;
    }

    public boolean isResolvedInFromClause() {
        return resolvedInFromClause;
    }

    @Override
    public String toString() {
        return name == null ? String.valueOf(cteMouldId) : name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCTE(this, context);
    }

    @Override
    public TableName getResolveTableName() {
        if (alias != null) {
            return alias;
        } else {
            return new TableName(null, name);
        }
    }
}
