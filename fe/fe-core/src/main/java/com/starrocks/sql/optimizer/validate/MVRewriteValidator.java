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

import com.google.common.base.Joiner;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.metric.MaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsRegistry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.PlannerProfile;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.metric.MaterializedViewMetricsEntity.isUpdateMaterializedViewMetrics;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.collectMaterializedViews;

public class MVRewriteValidator {
    private static final MVRewriteValidator INSTANCE = new MVRewriteValidator();

    public static MVRewriteValidator getInstance() {
        return INSTANCE;
    }

    /**
     * Record the MV usage into audit log
     */
    public void auditMv(ConnectContext connectContext, OptExpression physicalPlan, TaskContext taskContext) {
        if (!isUpdateMaterializedViewMetrics(connectContext)) {
            return;
        }

        OptimizerContext optimizerContext = taskContext.getOptimizerContext();
        // Candidate MVs
        if (CollectionUtils.isNotEmpty(optimizerContext.getCandidateMvs())) {
            List<String> mvNames = optimizerContext.getCandidateMvs()
                    .stream().map(x -> x.getMv().getName())
                    .collect(Collectors.toList());
            connectContext.getAuditEventBuilder().setCandidateMvs(mvNames);
        }

        // Rewritten MVs
        List<MaterializedView> mvs = collectMaterializedViews(physicalPlan);
        // To avoid queries that query the materialized view directly, only consider materialized views
        // that are not used in rewriting before.
        Set<Long> beforeTableIds = taskContext.getAllScanOperators().stream()
                .map(op -> op.getTable().getId())
                .collect(Collectors.toSet());
        if (CollectionUtils.isNotEmpty(mvs)) {
            List<String> rewrittenMVs = mvs.stream().filter(mv -> !beforeTableIds.contains(mv.getId()))
                    .map(mv -> mv.getName())
                    .collect(Collectors.toList());
            connectContext.getAuditEventBuilder().setHitMvs(rewrittenMVs);
        }
    }

    private void updateMaterializedViewMetricsIfNeeded(ConnectContext connectContext,
                                                       OptimizerContext optimizerContext,
                                                       List<MaterializedView> mvs,
                                                       Set<Long> beforeTableIds) {
        // ignore: explain queries
        if (!isUpdateMaterializedViewMetrics(connectContext)) {
            return;
        }
        // update considered metrics
        if (CollectionUtils.isNotEmpty(optimizerContext.getCandidateMvs())) {
            for (MaterializationContext mvContext : optimizerContext.getCandidateMvs()) {
                MaterializedView mv = mvContext.getMv();
                if (mv == null) {
                    continue;
                }
                MaterializedViewMetricsEntity mvEntity =
                        MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
                mvEntity.increaseQueryConsideredCount(1L);
            }
        }
        // update rewritten metrics
        for (MaterializedView mv : mvs) {
            // To avoid queries that query the materialized view directly, only consider materialized views
            // that are not used in rewriting before.
            MaterializedViewMetricsEntity mvEntity =
                    MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
            if (!beforeTableIds.contains(mv.getId())) {
                mvEntity.increaseQueryHitCount(1L);
            }
            mvEntity.increaseQueryMaterializedViewCount(1L);
        }
    }

    public void validateMV(ConnectContext connectContext, OptExpression physicalPlan, TaskContext taskContext) {
        if (connectContext == null) {
            return;
        }

        PlannerProfile.LogTracer tracer = PlannerProfile.getLogTracer("Summary");
        if (tracer == null) {
            return;
        }

        List<MaterializedView> mvs = collectMaterializedViews(physicalPlan);
        Set<Long> beforeTableIds = taskContext.getAllScanOperators().stream()
                .map(op -> op.getTable().getId())
                .collect(Collectors.toSet());

        updateMaterializedViewMetricsIfNeeded(connectContext, taskContext.getOptimizerContext(), mvs, beforeTableIds);

        List<MaterializedView> diffMVs = mvs.stream()
                .filter(mv -> !beforeTableIds.contains(mv.getId()))
                .collect(Collectors.toList());

        if (taskContext.getOptimizerContext().getQueryTables() != null) {
            taskContext.getOptimizerContext().getQueryTables().addAll(diffMVs);
        }

        if (diffMVs.isEmpty()) {
            Map<String, PlannerProfile.LogTracer> tracers = connectContext.getPlannerProfile().getTracers();
            boolean hasRewriteSuccess = tracers.values().stream()
                    .anyMatch(t -> t.getLogs().stream().anyMatch(
                            log -> StringUtils.contains(log, MaterializedViewRewriter.REWRITE_SUCCESS)));

            if (connectContext.getSessionVariable().isEnableMaterializedViewRewriteOrError()) {
                String errorMessage = hasRewriteSuccess ?
                        "no executable plan with materialized view for this sql in " +
                                connectContext.getSessionVariable().getMaterializedViewRewriteMode() + " mode " +
                                "because of cost." :
                        "no executable plan with materialized view for this sql in " +
                                connectContext.getSessionVariable().getMaterializedViewRewriteMode() + " mode.";
                throw new IllegalArgumentException(errorMessage);
            } else {
                String logMessage = hasRewriteSuccess ?
                        "Query has already been successfully rewritten, but it is not chosen as the best plan by cost." :
                        "Query cannot be rewritten, please check the trace logs to find more information.";
                tracer.log(logMessage);
            }
        } else {
            List<String> mvNames = diffMVs.stream().map(MaterializedView::getName).collect(Collectors.toList());
            String logMessage = "Query has already been successfully rewritten by: "
                    + Joiner.on(",").join(mvNames) + ".";
            tracer.log(logMessage);
        }
    }
}
