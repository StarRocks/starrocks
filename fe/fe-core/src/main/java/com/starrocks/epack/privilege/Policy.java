// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Type;
import com.starrocks.epack.sql.ast.PolicyType;

import java.util.List;

public class Policy {
    private final PolicyType policyType;
    private final Long policyId;
    private String name;
    private final DbUID dbUID;
    private final List<String> argNames;
    private final List<Type> argTypes;
    private final Type retType;
    private Expr policyExpression;
    private String comment;

    public Policy(PolicyType policyType, Long policyId,
                  String policyName, DbUID dbUID,
                  List<String> argNames, List<Type> argTypes, Type retType,
                  Expr policyExpression, String comment) {
        this.policyType = policyType;
        this.policyId = policyId;

        this.name = policyName;
        this.dbUID = dbUID;

        this.argNames = argNames;
        this.argTypes = argTypes;
        this.retType = retType;
        this.policyExpression = policyExpression;
        this.comment = comment;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DbUID getDbUID() {
        return dbUID;
    }

    public List<String> getArgNames() {
        return argNames;
    }

    public List<Type> getArgTypes() {
        return argTypes;
    }

    public Type getRetType() {
        return retType;
    }

    public Expr getPolicyExpression() {
        return policyExpression.clone();
    }

    public void setPolicyExpression(Expr policyExpression) {
        this.policyExpression = policyExpression;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}
