// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.starrocks.alter.AlterOpType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import org.apache.commons.lang.NotImplementedException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ModifyBrokerClause extends AlterClause {
    public enum ModifyOp {
        OP_ADD,
        OP_DROP,
        OP_DROP_ALL
    }

    private final ModifyOp op;
    private final String brokerName;
    private final List<String> hostPorts;

    protected Set<Pair<String, Integer>> hostPortPairs;

    public ModifyBrokerClause(ModifyOp op, String brokerName, List<String> hostPorts) {
        super(AlterOpType.ALTER_OTHER);
        this.op = op;
        this.brokerName = brokerName;
        this.hostPorts = hostPorts;
        this.hostPortPairs = new HashSet<>();
    }

    public static ModifyBrokerClause createAddBrokerClause(String brokerName, List<String> hostPorts) {
        return new ModifyBrokerClause(ModifyOp.OP_ADD, brokerName, hostPorts);
    }

    public static ModifyBrokerClause createDropBrokerClause(String brokerName, List<String> hostPorts) {
        return new ModifyBrokerClause(ModifyOp.OP_DROP, brokerName, hostPorts);
    }

    public static ModifyBrokerClause createDropAllBrokerClause(String brokerName) {
        return new ModifyBrokerClause(ModifyOp.OP_DROP_ALL, brokerName, null);
    }

    public ModifyOp getOp() {
        return op;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public List<String> getHostPorts() {
        return hostPorts;
    }

    public Set<Pair<String, Integer>> getHostPortPairs() {
        return hostPortPairs;
    }

    public void validateBrokerName() throws AnalysisException {
        if (Strings.isNullOrEmpty(brokerName)) {
            throw new AnalysisException("Broker's name can't be empty.");
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitModifyBrokerClause(this, context);
    }

    @Override
    public Map<String, String> getProperties() {
        throw new NotImplementedException();
    }
}
