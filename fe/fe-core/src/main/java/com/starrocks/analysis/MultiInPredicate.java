package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;

import java.util.List;

/**
 * Represents an IN subquery with multiple columns.
 */
public class MultiInPredicate extends Predicate{
    private final boolean isNotIn;
    // Expressions involved on the left side of the IN predicate (outside the subquery).
    List<Expr> outerExprs;
    Expr subquery;

    // Number of columns involved in the IN subquery.
    int tupleSize;

    public MultiInPredicate(List<Expr> outerExprs, Expr subquery, boolean isNotIn) {
        Preconditions.checkNotNull(subquery);
        children.addAll(outerExprs);
        children.add(subquery);
        this.outerExprs = outerExprs;
        this.subquery = subquery;
        this.isNotIn = isNotIn;
        this.tupleSize = outerExprs.size();

    }

    protected MultiInPredicate(MultiInPredicate other) {
        super(other);
        isNotIn = other.isNotIn();
        tupleSize = other.tupleSize;
    }

    @Override
    public Expr negate() {
        return new MultiInPredicate(this.outerExprs, this.subquery,
                !isNotIn);
    }

    public boolean isNotIn() {
        return isNotIn;
    }

    public int getTupleSize() {return tupleSize;}

    @Override
    public String toSqlImpl() {
        StringBuilder strBuilder = new StringBuilder();
        String notStr = (isNotIn) ? "NOT " : "";
        strBuilder.append("(");
        for (int i = 0; i < tupleSize; ++i) {
            strBuilder.append(getChild(i).toSql());
            if (i < tupleSize - 1) {
                strBuilder.append(", ");
            }
        }
        strBuilder.append(") ").append(notStr).append("IN (").append(getChild(tupleSize).toSql());
        strBuilder.append(")");
        return strBuilder.toString();
    }

    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            MultiInPredicate expr = (MultiInPredicate) obj;
            return isNotIn == expr.isNotIn && tupleSize == expr.tupleSize;
        }
        return false;
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public Expr clone() {
        return new MultiInPredicate(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitMultiInPredicate(this, context);
    }
}
