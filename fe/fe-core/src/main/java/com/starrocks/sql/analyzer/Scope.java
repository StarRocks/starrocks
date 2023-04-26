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

package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.PlaceHolderExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.LambdaArgument;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Scope represent the namespace used for resolved
 * scope include all fields in this namespace
 */
public class Scope {
    private Scope parent;
    private final RelationId relationId;
    private final RelationFields relationFields;
    private final Map<String, CTERelation> cteQueries = Maps.newLinkedHashMap();

    private List<PlaceHolderExpr> lambdaInputs = Lists.newArrayList();

    private boolean isLambdaScope = false;

    public Scope(RelationId relationId, RelationFields relation) {
        this.relationId = relationId;
        this.relationFields = relation;
    }

    public Scope(List<LambdaArgument> exprs, Scope parent) {
        this.relationId = RelationId.anonymous();
        List<Field> fieldList = Lists.newArrayList();
        for (int i = 0; i < exprs.size(); ++i) {
            fieldList.add(new Field(exprs.get(i).getName(), exprs.get(i).getType(),
                    new TableName(TableName.LAMBDA_FUNC_TABLE, TableName.LAMBDA_FUNC_TABLE), exprs.get(i), false));
        }
        relationFields = new RelationFields(fieldList);
        this.parent = parent;
        isLambdaScope = true;
    }

    public void putLambdaInput(PlaceHolderExpr expr) {
        lambdaInputs.add(expr);
    }

    public void clearLambdaInputs() {
        lambdaInputs.clear();
    }

    public List<PlaceHolderExpr> getLambdaInputs() {
        return lambdaInputs;
    }

    public RelationId getRelationId() {
        return relationId;
    }

    public boolean isLambdaScope() {
        return isLambdaScope;
    }

    public RelationFields getRelationFields() {
        return relationFields;
    }

    public Optional<ResolvedField> tryResolveField(SlotRef expression) {
        return resolveField(expression, 0, RelationId.anonymous());
    }

    public ResolvedField resolveField(SlotRef expression) {
        return resolveField(expression, RelationId.anonymous());
    }

    public ResolvedField resolveField(SlotRef expression, RelationId outerRelationId) {
        Optional<ResolvedField> resolvedField = resolveField(expression, 0, outerRelationId);
        if (!resolvedField.isPresent()) {
            throw new SemanticException("Column '%s' cannot be resolved", expression.toSql());
        }
        return resolvedField.get();
    }

    private Optional<ResolvedField> resolveField(SlotRef expression, int fieldIndexOffset, RelationId outerRelationId) {
        List<Field> matchFields = relationFields.resolveFields(expression);
        if (matchFields.size() > 1) {
            throw new SemanticException("Column '%s' is ambiguous", expression.getColumnName());
        } else if (matchFields.size() == 1) {
            if (matchFields.get(0).getType().getPrimitiveType().equals(PrimitiveType.UNKNOWN_TYPE)) {
                throw new SemanticException("Datatype of external table column [" + matchFields.get(0).getName()
                        + "] is not supported!");
            } else {
                return Optional.of(asResolvedField(matchFields.get(0), fieldIndexOffset));
            }
        } else {
            if (parent != null
                    //Correlated subqueries currently only support accessing properties in the first level outer layer
                    && !relationId.equals(outerRelationId)
                    || parent != null && isLambdaScope) { // also to analyze the nested lambda arguments.
                return parent.resolveField(expression, fieldIndexOffset + relationFields.getAllFields().size(),
                        outerRelationId);
            }
            return Optional.empty();
        }
    }

    public ResolvedField asResolvedField(Field field, int fieldIndexOffset) {
        int hierarchyFieldIndex = relationFields.indexOf(field) + fieldIndexOffset;
        return new ResolvedField(this, field, hierarchyFieldIndex);
    }

    public void addCteQueries(String name, CTERelation view) {
        cteQueries.put(name, view);
    }

    public Optional<CTERelation> getCteQueries(String name) {
        if (cteQueries.containsKey(name)) {
            return Optional.of(cteQueries.get(name));
        }

        if (parent != null) {
            return parent.getCteQueries(name);
        }

        return Optional.empty();
    }

    public boolean containsCTE(String name) {
        return cteQueries.containsKey(name);
    }

    public Scope getParent() {
        return parent;
    }

    public void setParent(Scope parent) {
        this.parent = parent;
    }
}
