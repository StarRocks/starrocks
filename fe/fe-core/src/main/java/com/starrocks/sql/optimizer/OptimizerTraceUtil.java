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

package com.starrocks.sql.optimizer;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.common.DebugRelationTracer;
import com.starrocks.sql.optimizer.rule.Rule;
import org.slf4j.helpers.MessageFormatter;

import java.util.List;

public class OptimizerTraceUtil {
    public static void logOptExpression(String format, OptExpression optExpression) {
        Tracers.log(Tracers.Module.OPTIMIZER, args -> String.format(format, optExpression.debugString()));
    }

    public static void logQueryStatement(String format, QueryStatement statement) {
        Tracers.log(Tracers.Module.ANALYZE,
                args -> String.format(format, statement.accept(new DebugRelationTracer(), "")));
    }

    public static void log(String format, Object... object) {
        Tracers.log(Tracers.Module.OPTIMIZER, args -> String.format(format, object));
    }

    public static void logMVPrepare(ConnectContext ctx, String format, Object... object) {
        logMVPrepare(ctx, null, format, object);
    }

    public static void logMVPrepare(ConnectContext ctx, MaterializedView mv,
                                    String format, Object... object) {
        Tracers.log(Tracers.Module.MV, input -> {
            String str = MessageFormatter.arrayFormat(format, object).getMessage();
            Object[] args = new Object[] {ctx.getQueryId(), mv == null ? "GLOBAL" : mv.getName(), str};
            return MessageFormatter.arrayFormat("[MV TRACE] [PREPARE {}][{}] {}", args).getMessage();
        });
    }

    public static void logMVRewrite(MvRewriteContext mvRewriteContext, String format, Object... object) {
        MaterializationContext mvContext = mvRewriteContext.getMaterializationContext();
        Tracers.log(Tracers.Module.MV, input -> {
            Object[] args = new Object[] {
                    mvContext.getOptimizerContext().getQueryId(),
                    mvRewriteContext.getRule().type().name(),
                    mvContext.getMv().getName(),
                    MessageFormatter.arrayFormat(format, object).getMessage()
            };
            return MessageFormatter.arrayFormat("[MV TRACE] [REWRITE {} {} {}] {}", args).getMessage();
        });
    }

    public static void logMVRewrite(OptimizerContext optimizerContext, Rule rule,
                                    String format, Object... object) {
        Tracers.log(Tracers.Module.MV, input -> {
            Object[] args = new Object[] {
                    optimizerContext.getQueryId(),
                    rule.type().name(),
                    String.format(format, object)
            };
            return MessageFormatter.arrayFormat("[MV TRACE] [REWRITE {} {}] {}", args).getMessage();
        });
    }

    public static void logApplyRule(OptimizerContext ctx, Rule rule,
                                    OptExpression oldExpression, List<OptExpression> newExpressions) {
        Tracers.log(Tracers.Module.OPTIMIZER, args -> {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("[TRACE QUERY %s] APPLY RULE %s\n", ctx.getQueryId(), rule));
            sb.append("Original Expression:\n").append(oldExpression.debugString(3));
            sb.append("\nNew Expression:");
            if (newExpressions.isEmpty()) {
                sb.append("Empty");
            } else {
                sb.append("\n");
                for (int i = 0; i < newExpressions.size(); i++) {
                    sb.append(i).append(":").append(newExpressions.get(i).debugString(3));
                }
            }
            return sb.toString();
        });
    }

}
