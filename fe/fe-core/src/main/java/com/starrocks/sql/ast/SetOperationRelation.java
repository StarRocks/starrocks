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

import com.starrocks.analysis.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;

public abstract class SetOperationRelation extends QueryRelation {
    private List<QueryRelation> relations;
    private final SetQualifier qualifier;

    protected SetOperationRelation(List<QueryRelation> relations, SetQualifier qualifier) {
        this(relations, qualifier, NodePosition.ZERO);
    }

    protected SetOperationRelation(List<QueryRelation> relations, SetQualifier qualifier, NodePosition pos) {
        super(pos);
        this.relations = new ArrayList<>(relations);
        this.qualifier = qualifier;
    }

    public List<QueryRelation> getRelations() {
        return relations;
    }

    public void addRelation(QueryRelation relation) {
        relations.add(relation);
    }

    public SetQualifier getQualifier() {
        return qualifier;
    }

    public void setRelations(List<QueryRelation> relations) {
        this.relations = relations;
    }

    @Override
    public List<Expr> getOutputExpression() {
        return relations.get(0).getOutputExpression();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetOp(this, context);
    }
}
