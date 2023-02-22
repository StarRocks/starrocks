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

import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public abstract class Relation implements ParseNode {
    private Scope scope;

    protected TableName alias;
    protected List<String> explicitColumnNames;

    protected final NodePosition pos;

    protected Relation(NodePosition pos) {
        this.pos = pos;
    }

    public Scope getScope() {
        if (scope == null) {
            throw new StarRocksPlannerException("Scope is null", ErrorType.INTERNAL_ERROR);
        }
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public RelationFields getRelationFields() {
        return scope.getRelationFields();
    }

    public void setAlias(TableName alias) {
        this.alias = alias;
    }

    public TableName getAlias() {
        return alias;
    }

    public TableName getResolveTableName() {
        return alias;
    }

    public List<String> getColumnOutputNames() {
        return explicitColumnNames;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public void setColumnOutputNames(List<String> explicitColumnNames) {
        this.explicitColumnNames = explicitColumnNames;
    }

    public List<String> getExplicitColumnNames() {
        return explicitColumnNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRelation(this, context);
    }
}