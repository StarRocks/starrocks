// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.stream.Collectors;

public class CreatePolicyStmt extends DdlStmt {
    private final boolean ifNotExists;
    private final PolicyType policyType;
    private final PolicyName policyName;
    private final List<String> argNames;
    private final List<TypeDef> argTypeDefs;
    private final TypeDef returnType;
    private final Expr expression;
    private final String comment;

    public CreatePolicyStmt(boolean ifNotExists, PolicyType policyType, PolicyName policyName,
                            List<String> argNames, List<TypeDef> argTypeDefs, TypeDef returnType,
                            Expr expression, String comment, NodePosition pos) {
        super(pos);
        this.ifNotExists = ifNotExists;
        this.policyType = policyType;
        this.policyName = policyName;
        this.argNames = argNames;
        this.argTypeDefs = argTypeDefs;
        this.returnType = returnType;
        this.expression = expression;
        this.comment = comment;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public List<String> getArgNames() {
        return argNames;
    }

    public List<Type> getArgTypeDefs() {
        return argTypeDefs.stream().map(TypeDef::getType).collect(Collectors.toList());
    }

    public TypeDef getReturnType() {
        return returnType;
    }

    public Expr getExpression() {
        return expression;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreatePolicyStatement(this, context);
    }
}
