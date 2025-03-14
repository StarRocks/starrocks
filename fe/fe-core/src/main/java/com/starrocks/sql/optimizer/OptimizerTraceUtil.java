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
            Object[] args = new Object[] {mv == null ? "GLOBAL" : mv.getName(), str};
            return MessageFormatter.arrayFormat("[MV TRACE] [PREPARE {}] {}", args).getMessage();
        });
    }

    public static void logMVPrepare(String format, Object... object) {
        Tracers.log(Tracers.Module.MV, input -> {
            String str = MessageFormatter.arrayFormat(format, object).getMessage();
            Object[] args = new Object[] {"GLOBAL", str};
            return MessageFormatter.arrayFormat("[MV TRACE] [PREPARE {}] {}", args).getMessage();
        });
    }

    public static void logMVPrepare(MaterializedView mv,
                                    String format, Object... object) {
        Tracers.log(Tracers.Module.MV, input -> {
            String str = MessageFormatter.arrayFormat(format, object).getMessage();
            Object[] args = new Object[] {mv == null ? "GLOBAL" : mv.getName(), str};
            return MessageFormatter.arrayFormat("[MV TRACE] [PREPARE {}] {}", args).getMessage();
        });
    }

    public static void logMVRewrite(String mvName, String format, Object... objects) {
        Tracers.log(Tracers.Module.MV, input -> {
            String str = MessageFormatter.arrayFormat(format, objects).getMessage();
            return MessageFormatter.format("[MV TRACE] [REWRITE {}] {}", mvName, str).getMessage();
        });
    }

    /**
     * NOTE: Carefully use it, because the log would be print into the query profile, to help understanding why a
     * materialized view is not chose to rewrite the query.
     *
     * Used for mv preprocessor log, the log would be print into the query profile.
     */
    public static void logMVRewriteFailReason(String mvName, String format, Object... objects) {
        String str = MessageFormatter.arrayFormat(format, objects).getMessage();
        Tracers.reasoning(Tracers.Module.MV, "MV rewrite fail for {}: {} ", mvName, str);
        logMVRewrite(mvName, format, objects);
    }

    /**
     * Used for mv rewrite reason log, the log would be print into the query profile.
     */
    public static void logMVRewriteFailReason(MvRewriteContext mvContext, String format, Object... objects) {
        final String mvName = mvContext.getMVName();
        final String str = MessageFormatter.arrayFormat(format, objects).getMessage();
        final OptimizerContext optimizerContext = mvContext.getMaterializationContext().getOptimizerContext();
        final String memoPhase = optimizerContext.isInMemoPhase() ? "CBO" : "RBO";
        final String stage = optimizerContext.getQueryMaterializationContext().getCurrentRewriteStage().name();
        Tracers.reasoning(Tracers.Module.MV, "[{}] [{}] MV rewrite fail for {}: {} ", memoPhase, stage, mvName, str);
        logMVRewrite(mvName, format, objects);
    }

    public static void logMVRewrite(MaterializationContext mvContext, String format, Object... object) {
        Tracers.log(Tracers.Module.MV, input -> {
            Object[] args = new Object[] {
                    mvContext.getOptimizerContext().isInMemoPhase(),
                    mvContext.getMv().getName(),
                    MessageFormatter.arrayFormat(format, object).getMessage()
            };
            return MessageFormatter.arrayFormat("[MV TRACE] [REWRITE] [InMemo:{}] [{}] {}",
                    args).getMessage();
        });
    }

    public static void logMVRewrite(MvRewriteContext mvRewriteContext, String format, Object... object) {
        MaterializationContext mvContext = mvRewriteContext.getMaterializationContext();
        Tracers.log(Tracers.Module.MV, input -> {
            Object[] args = new Object[] {
                    mvRewriteContext.getRule().type().name(),
                    mvRewriteContext.getMaterializationContext().getOptimizerContext().isInMemoPhase(),
                    mvContext.getMv().getName(),
                    MessageFormatter.arrayFormat(format, object).getMessage()
            };
            return MessageFormatter.arrayFormat("[MV TRACE] [REWRITE {}] [InMemo:{}] [{}] {}",
                    args).getMessage();
        });
    }

    public static void logMVRewrite(OptimizerContext optimizerContext, Rule rule,
                                    String format, Object... object) {
        Tracers.log(Tracers.Module.MV, input -> {
            Object[] args = new Object[] {
                    rule.type().name(),
                    optimizerContext.isInMemoPhase(),
                    MessageFormatter.arrayFormat(format, object).getMessage()
            };
            return MessageFormatter.arrayFormat("[MV TRACE] [REWRITE {}] [InMemo:{}] {}", args).getMessage();
        });
    }

    public static void logMVRewriteRule(String ruleName, String format, Object... object) {
        Tracers.log(Tracers.Module.MV, input -> {
            Object[] args = new Object[] {
                    ruleName,
                    MessageFormatter.arrayFormat(format, object).getMessage()
            };
            return MessageFormatter.arrayFormat("[MV TRACE] [REWRITE {}] {}", args).getMessage();
        });
    }

    public static void logRuleExhausted(OptimizerContext ctx, Rule rule) {
        Tracers.log(Tracers.Module.OPTIMIZER,
                args -> String.format("[TRACE QUERY %s] RULE %s exhausted \n", ctx.getQueryId(), rule));
    }

    public static void logApplyRuleBefore(OptimizerContext ctx, Rule rule,
                                          OptExpression oldExpression) {
        Tracers.log(Tracers.Module.OPTIMIZER, args -> {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("[TRACE QUERY %s] APPLY RULE %s\n", ctx.getQueryId(), rule));
            sb.append("Original Expression:\n").append(oldExpression.debugString());
            return sb.toString();
        });
    }

    public static void logApplyRuleAfter(List<OptExpression> newExpressions) {
        Tracers.log(Tracers.Module.OPTIMIZER, args -> {
            StringBuilder sb = new StringBuilder();
            sb.append("\nNew Expression:");
            if (newExpressions.isEmpty()) {
                sb.append("Empty");
            } else {
                sb.append("\n");
                for (int i = 0; i < newExpressions.size(); i++) {
                    sb.append(i).append(":").append(newExpressions.get(i).debugString());
                }
            }
            sb.append("\n");
            return sb.toString();
        });
    }
}
