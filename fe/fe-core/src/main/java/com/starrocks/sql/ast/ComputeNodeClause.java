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

import com.starrocks.alter.AlterOpType;
import com.starrocks.common.Pair;
import com.starrocks.sql.parser.NodePosition;

import java.util.LinkedList;
import java.util.List;

public class ComputeNodeClause extends AlterClause {

    protected List<String> hostPorts;
    private final List<Pair<String, Integer>> hostPortPairs;

    public ComputeNodeClause(List<String> hostPorts, NodePosition pos) {
        super(AlterOpType.ALTER_OTHER, pos);
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
