// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.Relation;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

/**
 * RelationId is used to uniquely mark a Relation,which is mainly used in
 * FieldId to distinguish which relation the field comes from when resolve
 */
public class RelationId {
    private final Relation sourceNode;

    private RelationId(Relation sourceNode) {
        this.sourceNode = sourceNode;
    }

    public static RelationId anonymous() {
        return new RelationId(null);
    }

    public static RelationId of(Relation sourceNode) {
        return new RelationId(requireNonNull(sourceNode, "source cannot be null"));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RelationId that = (RelationId) o;
        return sourceNode != null && sourceNode == that.sourceNode;
    }

    @Override
    public int hashCode() {
        return identityHashCode(sourceNode);
    }

    @Override
    public String toString() {
        if (sourceNode == null) {
            return toStringHelper(this)
                    .addValue("anonymous")
                    .addValue(format("x%08x", identityHashCode(this)))
                    .toString();
        } else {
            return toStringHelper(this)
                    .addValue(sourceNode.getClass().getSimpleName())
                    .addValue(format("x%08x", identityHashCode(sourceNode)))
                    .toString();
        }
    }
}
