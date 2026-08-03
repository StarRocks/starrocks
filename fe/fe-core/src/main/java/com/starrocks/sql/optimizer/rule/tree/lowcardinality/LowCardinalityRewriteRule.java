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

package com.starrocks.sql.optimizer.rule.tree.lowcardinality;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.rule.tree.TreeRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;

public class LowCardinalityRewriteRule implements TreeRewriteRule {

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        OptimizerContext optimizerContext = taskContext.getOptimizerContext();
        SessionVariable session = optimizerContext.getSessionVariable();
        ConnectContext connectContext = optimizerContext.getConnectContext();
        boolean isQuery = connectContext.getState().isQuery();
        if (!session.isEnableLowCardinalityOptimize() || !session.isUseLowCardinalityOptimizeV2()) {
            return root;
        }

        // Capture the accepted global dicts into the query dump so offline replay reproduces the Decode.
        // Capturing here (not during scan statistics) makes the captured dict exactly the one the rewrite
        // selects, avoiding the stats-vs-rewrite race. Gate on a real QueryDumpInfo rather than
        // shouldDumpQuery(): that also covers the failure/virtual dump (ExecuteExceptionHandler#buildVirtualDump
        // installs a QueryDumpInfo and replans without setting the dump session flags), while staying inert for
        // normal queries (no DumpInfo) and unit tests (the harness installs a no-op MockDumpInfo).
        boolean captureForDump = connectContext.getDumpInfo() instanceof QueryDumpInfo;

        ColumnRefFactory factory = optimizerContext.getColumnRefFactory();
        DecodeContext context = new DecodeContext(factory);
        DecodeCollector collector = new DecodeCollector(session, isQuery, captureForDump);
        collector.collect(root, context);
        if (!collector.isValidMatchChildren()) {
            return root;
        }
        if (captureForDump) {
            DumpInfo dumpInfo = connectContext.getDumpInfo();
            for (DecodeCollector.CapturedGlobalDict captured : collector.getCapturedGlobalDicts()) {
                dumpInfo.addTableGlobalDict(captured.table, captured.columnName, captured.dict);
            }
        }
        DecodeRewriter rewriter = new DecodeRewriter(factory, context, session);
        return rewriter.rewrite(root);
    }
}