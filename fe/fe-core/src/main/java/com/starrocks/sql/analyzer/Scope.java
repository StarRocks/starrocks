// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Maps;
import com.starrocks.analysis.SlotRef;
import com.starrocks.sql.analyzer.relation.QueryRelation;

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

    private Map<String, QueryRelation> cteQueries = Maps.newLinkedHashMap();

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
        return resolveField(expression, 0);
    }

    public ResolvedField resolveField(SlotRef expression) {
        Optional<ResolvedField> resolvedField = resolveField(expression, 0);
        if (!resolvedField.isPresent()) {
            throw new SemanticException("Column '%s' cannot be resolved", expression.toSql());
        }
        return resolvedField.get();
    }

    private Optional<ResolvedField> resolveField(SlotRef expression, int fieldIndexOffset) {
        List<Field> matchFields = relationFields.resolveFields(expression);
        if (matchFields.size() > 1) {

            boolean sameField = true;
            for (int i = 0; i < matchFields.size() - 1; ++i) {
                if (matchFields.get(i).getOriginExpression() != null &&
                        matchFields.get(i + 1).getOriginExpression() != null) {
                    if (!matchFields.get(i).getOriginExpression()
                            .equals(matchFields.get(i + 1).getOriginExpression())) {
                        sameField = false;
                    }
                }

                if (matchFields.get(i).getRelationAlias() != null &&
                        matchFields.get(i + 1).getRelationAlias() != null) {
                    if (!matchFields.get(i).getRelationAlias().equals(matchFields.get(i + 1).getRelationAlias())) {
                        sameField = false;
                    }
                }
            }
            if (sameField) {
                return Optional.of(asResolvedField(matchFields.get(0), fieldIndexOffset));
            }

            throw new SemanticException("Column '%s' is ambiguous", expression.getColumnName());
        } else if (matchFields.size() == 1) {
            return Optional.of(asResolvedField(matchFields.get(0), fieldIndexOffset));
        } else {
            if (parent != null) {
                return parent.resolveField(expression, fieldIndexOffset + relationFields.getAllFields().size());
            }
            return Optional.empty();
        }
    }

    public ResolvedField asResolvedField(Field field, int fieldIndexOffset) {
        int hierarchyFieldIndex = relationFields.indexOf(field) + fieldIndexOffset;
        return new ResolvedField(this, field, hierarchyFieldIndex);
    }

    public void addCteQueries(String name, QueryRelation view) {
        cteQueries.put(name, view);
    }

    public Optional<QueryRelation> getCteQueries(String name) {
        if (cteQueries.containsKey(name)) {
            return Optional.of(cteQueries.get(name));
        }

        if (parent != null) {
            return parent.getCteQueries(name);
        }

        return Optional.empty();
    }

    public Map<String, QueryRelation> getAllCteQueries() {
        return cteQueries;
    }

    public Scope getParent() {
        return parent;
    }

    public void setParent(Scope parent) {
        this.parent = parent;
    }
}
