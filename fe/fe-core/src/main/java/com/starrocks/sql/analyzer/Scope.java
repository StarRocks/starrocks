// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Maps;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.sql.ast.CTERelation;

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

    public Scope(RelationId relationId, RelationFields relation) {
        this.relationId = relationId;
        this.relationFields = relation;
    }

    public RelationId getRelationId() {
        return relationId;
    }

    public RelationFields getRelationFields() {
        return relationFields;
    }

    public Optional<ResolvedField> tryResolveFeild(SlotRef expression) {
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
                    && !relationId.equals(outerRelationId)) {
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

    public Scope getParent() {
        return parent;
    }

    public void setParent(Scope parent) {
        this.parent = parent;
    }
}
