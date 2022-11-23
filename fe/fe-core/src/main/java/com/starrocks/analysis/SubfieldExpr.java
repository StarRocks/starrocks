// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

public class SubfieldExpr extends Expr {

    // We use fieldName to extract subfield column from children[0],
    // children[0] must be an StructType.
    private final String fieldName;

    // Only used in parser, in parser, we can't determine column's type
    public SubfieldExpr(Expr child, String fieldName) {
        this(child, null, fieldName);
    }

    // In this constructor, we can determine column's type
    // child must be an StructType
    public SubfieldExpr(Expr child, Type type, String fieldName) {
        if (type != null) {
            Preconditions.checkArgument(child.getType().isStructType());
        }
        children.add(child);
        this.type = type;
        this.fieldName = fieldName.toLowerCase();
    }

    public SubfieldExpr(SubfieldExpr other) {
        super(other);
        fieldName = other.fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public int getFieldPos(String fieldName) {
        return ((StructType) (children.get(0).getType())).getFieldPos(fieldName);
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSubfieldExpr(this, context);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(false, "unreachable");
    }

    @Override
    protected String toSqlImpl() {
        return getChild(0).toSqlImpl() + "." + fieldName;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.SUBFIELD_EXPR);
        msg.setUsed_subfield_name(fieldName);
    }

    @Override
    public Expr clone() {
        return new SubfieldExpr(this);
    }
}
