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

import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class MvPlanContextBuilder {
    private static final Logger LOG = LogManager.getLogger(MvPlanContextBuilder.class);

    /**
     * Get plan context for the given materialized view.
     * @param mv
     * @param isThrowException: whether to throw exception when failed to build plan context:
     *                        - when in altering table, we want to throw exception to fail the alter table operation
     *                        - when in generating mv plan, we want to ignore the exception and continue the query
     */
    public static List<MvPlanContext> getPlanContext(MaterializedView mv,
                                                     boolean isThrowException) {
        // build mv query logical plan
        MaterializedViewOptimizer mvOptimizer = new MaterializedViewOptimizer();

        // If the caller is not from query (eg. background schema change thread), set thread local info to avoid
        // NPE in the planning.
        ConnectContext connectContext = ConnectContext.get() == null ? new ConnectContext() : ConnectContext.get();

        List<MvPlanContext> results = Lists.newArrayList();
        try (var guard = connectContext.bindScope()) {
            Optional.ofNullable(doGetOptimizePlan(() -> mvOptimizer.optimize(mv, connectContext), isThrowException))
                    .map(results::add);

            // TODO: Only add context with view when view rewrite is set on.
            if (mv.getBaseTableTypes().stream().anyMatch(type -> type == TableType.VIEW)) {
                Optional.ofNullable(doGetOptimizePlan(() -> mvOptimizer.optimize(mv, connectContext, false, true),
                                isThrowException))
                        .map(results::add);
            }
        }
        return results;
    }

    private static MvPlanContext doGetOptimizePlan(Supplier<MvPlanContext> supplier,
                                                   boolean isThrowException) {
        try {
            return supplier.get();
        } catch (Exception e) {
            // ignore
            LOG.warn("Failed to build mv plan context", e);
            if (isThrowException) {
                throw e;
            }
        }
        return null;
    }
}
