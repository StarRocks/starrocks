// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.common.Pair;
import org.apache.commons.lang.NotImplementedException;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BackendClause extends AlterClause {
    protected List<String> hostPorts;

    protected List<Pair<String, Integer>> hostPortPairs;

    protected BackendClause(List<String> hostPorts) {
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
    public Map<String, String> getProperties() {
        throw new NotImplementedException();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBackendClause(this, context);
    }
}
