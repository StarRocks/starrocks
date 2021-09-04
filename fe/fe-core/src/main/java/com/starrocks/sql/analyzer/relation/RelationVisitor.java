// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer.relation;

public abstract class RelationVisitor<R, C> {
    public R visit(Relation relation) {
        return visit(relation, null);
    }

    public R visit(Relation relation, C context) {
        return relation.accept(this, context);
    }

    public R visitQuery(QueryRelation node, C context) {
        return null;
    }

    public R visitQuerySpecification(QuerySpecification node, C context) {
        return null;
    }

    public R visitTable(TableRelation node, C context) {
        return null;
    }

    public R visitJoin(JoinRelation node, C context) {
        return null;
    }

    public R visitSubquery(SubqueryRelation node, C context) {
        return null;
    }

    public R visitUnion(UnionRelation node, C context) {
        return null;
    }

    public R visitExcept(ExceptRelation node, C context) {
        return null;
    }

    public R visitIntersect(IntersectRelation node, C context) {
        return null;
    }

    public R visitValues(ValuesRelation node, C context) {
        return null;
    }

    public R visitTableFunction(TableFunctionRelation node, C context) {
        return null;
    }

    public R visitInsert(InsertRelation node, C context) {
        return null;
    }
}