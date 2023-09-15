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

package com.starrocks.sql.optimizer.validate;

import com.google.api.client.util.Lists;
import com.google.common.base.Joiner;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.profile.Var;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

public class MVRewriteValidator {
    private static final MVRewriteValidator INSTANCE = new MVRewriteValidator();

    public static MVRewriteValidator getInstance() {
        return INSTANCE;
    }

    /**
     * Record the MV usage into audit log
     */
    public void auditMv(OptExpression physicalPlan, OptimizerContext optimizerContext) {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            return;
        }

        // Candidate MVs
        if (CollectionUtils.isNotEmpty(optimizerContext.getCandidateMvs())) {
            List<String> mvNames = optimizerContext.getCandidateMvs()
                    .stream().map(x -> x.getMv().getName())
                    .collect(Collectors.toList());
            connectContext.getAuditEventBuilder().setCandidateMvs(mvNames);
        }

        // Rewritten MVs
        List<String> mvNames = MvUtils.collectMaterializedViewNames(physicalPlan);
        if (CollectionUtils.isNotEmpty(mvNames)) {
            connectContext.getAuditEventBuilder().setHitMvs(mvNames);
        }
    }

    public void validateMV(OptExpression physicalPlan) {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            return;
        }

        List<String> mvNames = MvUtils.collectMaterializedViewNames(physicalPlan);
        if (mvNames.isEmpty()) {
            // Check whether plan has been rewritten success by rule.

            List<String> tracerNames = Lists.newArrayList();
            for (Var<?> var : Tracers.getAllVars()) {
                if (StringUtils.contains(var.getValue().toString(), MaterializedViewRewriter.REWRITE_SUCCESS)) {
                    tracerNames.add(var.getName().replace("REWRITE ", ""));
                }
            }

            if (connectContext.getSessionVariable().isEnableMaterializedViewRewriteOrError()) {
                if (tracerNames.isEmpty()) {
                    throw new IllegalArgumentException("no executable plan with materialized view for this sql in " +
                            connectContext.getSessionVariable().getMaterializedViewRewriteMode() + " mode.");
                } else {
                    throw new IllegalArgumentException("no executable plan with materialized view for this sql in " +
                            connectContext.getSessionVariable().getMaterializedViewRewriteMode() + " mode because of" +
                            "cost.");
                }
            }

            if (tracerNames.isEmpty()) {
                Tracers.log(Tracers.Module.MV, "Query cannot be rewritten, please check the trace logs or " +
                        "`set enable_mv_optimizer_trace_log=on` to find more infos.");
            } else {
                Tracers.log(Tracers.Module.MV,
                        "Query has already been successfully rewritten by: " + Joiner.on(",").join(tracerNames)
                                + ", but are not chosen as the best plan by cost.");
            }
        } else {
            // If final result contains materialized views, ho
            if (connectContext.getSessionVariable().isEnableMaterializedViewRewriteOrError()) {
                boolean hasRewriteSuccess = false;
                for (Var<?> var : Tracers.getAllVars()) {
                    if (StringUtils.contains(var.getValue().toString(), MaterializedViewRewriter.REWRITE_SUCCESS)) {
                        hasRewriteSuccess = true;
                        break;
                    }
                }

                if (!hasRewriteSuccess) {
                    throw new IllegalArgumentException("no executable plan with materialized view for this sql in " +
                            connectContext.getSessionVariable().getMaterializedViewRewriteMode() + " mode.");
                }
            }

            Tracers.log(Tracers.Module.MV,
                    "Query has already been successfully rewritten by: " + Joiner.on(",").join(mvNames) + ".");
        }
    }


}
