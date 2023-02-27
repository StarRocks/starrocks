// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.starrocks.alter.AlterOpType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.sql.parser.NodePosition;
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

    public ModifyBrokerClause(ModifyOp op, String brokerName, List<String> hostPorts, NodePosition pos) {
        super(AlterOpType.ALTER_OTHER, pos);
        this.op = op;
        this.brokerName = brokerName;
        this.hostPorts = hostPorts;
        this.hostPortPairs = new HashSet<>();
    }

    public static ModifyBrokerClause createAddBrokerClause(String brokerName, List<String> hostPorts, NodePosition pos) {
        return new ModifyBrokerClause(ModifyOp.OP_ADD, brokerName, hostPorts, pos);
    }


    public static ModifyBrokerClause createDropBrokerClause(String brokerName, List<String> hostPorts, NodePosition pos) {
        return new ModifyBrokerClause(ModifyOp.OP_DROP, brokerName, hostPorts, pos);
    }

    public static ModifyBrokerClause createDropAllBrokerClause(String brokerName, NodePosition pos) {
        return new ModifyBrokerClause(ModifyOp.OP_DROP_ALL, brokerName, null, pos);
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
