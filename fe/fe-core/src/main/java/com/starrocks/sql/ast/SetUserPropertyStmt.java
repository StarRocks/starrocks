// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.common.Pair;

import java.util.List;

public class SetUserPropertyStmt extends DdlStmt {
    private String user;
    private final List<SetVar> propertyList;

    public SetUserPropertyStmt(String user, List<SetVar> propertyList) {
        this.user = user;
        this.propertyList = propertyList;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public List<SetVar> getPropertyList() {
        return this.propertyList;
    }

    // using List because we need retain the origin property order
    public List<Pair<String, String>> getPropertyPairList() {
        List<Pair<String, String>> list = Lists.newArrayList();
        for (SetVar var : propertyList) {
            list.add(Pair.create(((SetUserPropertyVar) var).getPropertyKey(),
                    ((SetUserPropertyVar) var).getPropertyValue()));
        }
        return list;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetUserPropertyStatement(this, context);
    }
}

