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

import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.FieldId;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class QueryRelation extends Relation {
    protected List<OrderByElement> sortClause;
    protected LimitElement limit;
    private final List<CTERelation> cteRelations = new ArrayList<>();

    protected QueryRelation() {
        this(NodePosition.ZERO);
    }

    protected QueryRelation(NodePosition pos) {
        super(pos);
    }

    @Override
    public List<String> getColumnOutputNames() {
        if (explicitColumnNames != null) {
            return explicitColumnNames;
        } else {
            return getScope().getRelationFields().getAllFields().stream().map(Field::getName).collect(Collectors.toList());
        }
    }

    public Map<Expr, FieldId> getColumnReferences() {
        return Maps.newHashMap();
    }

    public void setOrderBy(List<OrderByElement> sortClause) {
        this.sortClause = sortClause;
    }

    public List<OrderByElement> getOrderBy() {
        return sortClause;
    }

    public List<Expr> getOrderByExpressions() {
        return sortClause.stream().map(OrderByElement::getExpr).collect(Collectors.toList());
    }

    public boolean hasOrderByClause() {
        return sortClause != null && !sortClause.isEmpty();
    }

    public void clearOrder() {
        sortClause.clear();
    }

    public LimitElement getLimit() {
        return limit;
    }

    public void setLimit(LimitElement limit) {
        this.limit = limit;
    }

    public boolean hasLimit() {
        return limit != null;
    }

    public long getOffset() {
        return limit.getOffset();
    }

    public boolean hasOffset() {
        return limit != null && limit.hasOffset();
    }

    public boolean hasWithClause() {
        return !cteRelations.isEmpty();
    }

    public void addCTERelation(CTERelation cteRelation) {
        this.cteRelations.add(cteRelation);
    }

    public List<CTERelation> getCteRelations() {
        return cteRelations;
    }

    public abstract List<Expr> getOutputExpression();

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQueryRelation(this, context);
    }
}
