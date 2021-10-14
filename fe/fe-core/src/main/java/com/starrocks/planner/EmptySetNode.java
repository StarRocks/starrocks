// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/EmptySetNode.java

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

import com.google.common.base.Preconditions;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.TupleId;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.ArrayList;

/**
 * Node that returns an empty result set. Used for planning query blocks with a constant
 * predicate evaluating to false or a limit 0. The result set will have zero rows, but
 * the row descriptor must still include a materialized tuple so that the backend can
 * construct a valid row empty batch.
 */
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class EmptySetNode extends PlanNode {
    public EmptySetNode(PlanNodeId id, ArrayList<TupleId> tupleIds) {
        super(id, tupleIds, "EMPTYSET");
        Preconditions.checkArgument(tupleIds.size() > 0);
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        avgRowSize = 0;
        cardinality = 0;
        numNodes = 1;
    }

    @Override
    public void init(Analyzer analyzer) {
        Preconditions.checkState(conjuncts.isEmpty());
        // If the physical output tuple produced by an AnalyticEvalNode wasn't created
        // the logical output tuple is returned by getMaterializedTupleIds(). It needs
        // to be set as materialized (even though it isn't) to avoid failing precondition
        // checks generating the thrift for slot refs that may reference this tuple.
        for (TupleId id : tupleIds) {
            analyzer.getTupleDesc(id).setIsMaterialized(true);
        }
        computeMemLayout(analyzer);
        computeStats(analyzer);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.EMPTY_SET_NODE;
    }
}
