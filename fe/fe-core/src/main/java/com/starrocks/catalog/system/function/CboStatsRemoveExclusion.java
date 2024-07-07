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

package com.starrocks.catalog.system.function;

import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.List;

public class CboStatsRemoveExclusion implements GenericFunction {
    public static final String FN_NAME = FunctionSet.CBO_STATS_REMOVE_EXCLUSION;

    @Override
    public void init(FunctionCallExpr node, ConnectContext context) {
        genericSystemFunctionCheck(node);
        if (node.getChildren().size() != 1) {
            throw new SemanticException(FN_NAME + " input parameter must be 1", node.getPos());
        }

        if (!(node.getChild(0) instanceof IntLiteral)) {
            throw new SemanticException(FN_NAME + " input parameter must be int literal",
                    node.getPos());
        }
    }

    @Override
    public void prepare(FunctionCallExpr functionCallExpr, ConnectContext context) {
        //do nothing,refer to the behavior of 'DROP ANALYZE <ID>'
    }

    @Override
    public ConstantOperator evaluate(List<ConstantOperator> arguments) {
        long exclustId = arguments.get(0).getBigint();
        ErrorReport.wrapWithRuntimeException(
                () -> GlobalStateMgr.getCurrentState().getAnalyzeMgr().removeAnalyzeJob(exclustId));

        return ConstantOperator.createVarchar(Long.toString(exclustId));
    }
}
