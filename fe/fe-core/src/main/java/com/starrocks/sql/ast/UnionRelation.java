// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.sql.optimizer.base.SetQualifier;

import java.util.List;

public class UnionRelation extends SetOperationRelation {
    public UnionRelation(List<QueryRelation> relations, SetQualifier qualifier) {
        super(relations, qualifier);
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUnion(this, context);
    }

    @Override
    public String toPrettyString(String indent) {
        StringBuilder sb = new StringBuilder("UnionRelation{\n");
        sb.append(indent).append("relations=\n");
        for (QueryRelation relation : getRelations()) {
            sb.append(indent + "  ").append(relation.toPrettyString(indent + "  ")).append("\n");
        }
        sb.append("}");
        return sb.toString();
    }
}
