// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/ExceptNode.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.planner;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TupleId;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.List;

// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class ExceptNode extends SetOperationNode {
    public ExceptNode(PlanNodeId id, TupleId tupleId) {
        super(id, tupleId, "EXCEPT");
    }

    protected ExceptNode(PlanNodeId id, TupleId tupleId,
                         List<Expr> setOpResultExprs, boolean isInSubplan) {
        super(id, tupleId, "EXCEPT", setOpResultExprs, isInSubplan);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        toThrift(msg, TPlanNodeType.EXCEPT_NODE);
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }
}
