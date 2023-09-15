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
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.PlannerProfile;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;
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

        PlannerProfile.LogTracer tracer = PlannerProfile.getLogTracer("Summary");
        if (tracer == null) {
            return;
        }
        List<String> mvNames = MvUtils.collectMaterializedViewNames(physicalPlan);
        if (mvNames.isEmpty()) {
            // Check whether plan has been rewritten success by rule.
            Map<String, PlannerProfile.LogTracer> tracers = connectContext.getPlannerProfile().getTracers();
            List<String> tracerNames = Lists.newArrayList();
            for (Map.Entry<String, PlannerProfile.LogTracer> e : tracers.entrySet()) {
                if (e.getValue().getLogs().stream().anyMatch(x -> x.contains(MaterializedViewRewriter.REWRITE_SUCCESS))) {
                    tracerNames.add(e.getKey().replace("REWRITE ", ""));
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
                tracer.log("Query cannot be rewritten, please check the trace logs or " +
                        "`set enable_mv_optimizer_trace_log=on` to find more infos.");
            } else {
                tracer.log("Query has already been successfully rewritten by: " + Joiner.on(",").join(tracerNames)
                        + ", but are not chosen as the best plan by cost.");
            }
        } else {
            // If final result contains materialized views, ho
            if (connectContext.getSessionVariable().isEnableMaterializedViewRewriteOrError()) {
                Map<String, PlannerProfile.LogTracer> tracers = connectContext.getPlannerProfile().getTracers();
                if (tracers.entrySet().stream().noneMatch(e -> e.getValue().getLogs().stream()
                        .anyMatch(x -> x.contains(MaterializedViewRewriter.REWRITE_SUCCESS)))) {
                    throw new IllegalArgumentException("no executable plan with materialized view for this sql in " +
                            connectContext.getSessionVariable().getMaterializedViewRewriteMode() + " mode.");
                }
            }

            tracer.log("Query has already been successfully rewritten by: " + Joiner.on(",").join(mvNames) + ".");
        }
    }


}
