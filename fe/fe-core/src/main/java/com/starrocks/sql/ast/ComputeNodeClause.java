// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.common.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ComputeNodeClause extends AlterClause {

    protected Map<String, String> properties;
    protected List<String> hostPorts;
    private final List<Pair<String, Integer>> hostPortPairs;

    public ComputeNodeClause(List<String> hostPorts) {
        this(hostPorts, new HashMap<>());
    }

    public ComputeNodeClause(List<String> hostPorts, Map<String, String> properties) {
        super(AlterOpType.ALTER_OTHER);
        this.properties = properties;
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
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitComputeNodeClause(this, context);
    }
}
