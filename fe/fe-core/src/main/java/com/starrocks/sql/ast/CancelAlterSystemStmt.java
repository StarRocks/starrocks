// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.common.Pair;

import java.util.LinkedList;
import java.util.List;

public class CancelAlterSystemStmt extends CancelStmt {

    protected List<String> hostPorts;
    private final List<Pair<String, Integer>> hostPortPairs;

    public CancelAlterSystemStmt(List<String> hostPorts) {
        this.hostPorts = hostPorts;
        this.hostPortPairs = new LinkedList<>();
    }

    public List<String> getHostPorts() {
        return hostPorts;
    }

    public List<Pair<String, Integer>> getHostPortPairs() {
        return hostPortPairs;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCancelAlterSystemStatement(this, context);
    }
}
