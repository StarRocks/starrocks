// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.common.Pair;

import java.util.LinkedList;
import java.util.List;

public class ComputeNodeClause extends AlterClause {

    protected List<String> hostPorts;
    private final List<Pair<String, Integer>> hostPortPairs;

    public ComputeNodeClause(List<String> hostPorts) {
        super(AlterOpType.ALTER_OTHER);
        this.hostPorts = hostPorts;
        this.hostPortPairs = new LinkedList<>();
    }

    public List<Pair<String, Integer>> getHostPortPairs() {
        return hostPortPairs;
    }

    public List<String> getHostPorts() {
        return hostPorts;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitComputeNodeClause(this, context);
    }
}
