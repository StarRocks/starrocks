// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer.relation;

import com.starrocks.analysis.Expr;
import com.starrocks.sql.optimizer.base.SetQualifier;

import java.util.List;

public class IntersectRelation extends SetOperationRelation {
    public IntersectRelation(List<QueryRelation> relations, SetQualifier qualifier, List<Expr> outputExpressions) {
        super(relations, qualifier, outputExpressions);
    }

    public <R, C> R accept(RelationVisitor<R, C> visitor, C context) {
        return visitor.visitIntersect(this, context);
    }
}
