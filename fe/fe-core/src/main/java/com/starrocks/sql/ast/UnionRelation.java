// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import java.util.List;

public class UnionRelation extends SetOperationRelation {
    public UnionRelation(List<QueryRelation> relations, SetQualifier qualifier) {
        super(relations, qualifier);
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUnion(this, context);
    }
}
