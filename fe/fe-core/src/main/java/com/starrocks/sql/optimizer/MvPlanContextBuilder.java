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

import java.util.List;

public class MvPlanContextBuilder {
    public static List<MvPlanContext> getPlanContext(MaterializedView mv) {
        // build mv query logical plan
        MaterializedViewOptimizer mvOptimizer = new MaterializedViewOptimizer();

        // If the caller is not from query (eg. background schema change thread), set thread local info to avoid
        // NPE in the planning.
        ConnectContext connectContext = ConnectContext.get() == null ? new ConnectContext() : ConnectContext.get();

        List<MvPlanContext> results = Lists.newArrayList();
        try (var guard = connectContext.bindScope()) {
            MvPlanContext contextWithoutView = mvOptimizer.optimize(mv, connectContext);
            results.add(contextWithoutView);

            // TODO: Only add context with view when view rewrite is set on.
            if (mv.getBaseTableTypes().stream().anyMatch(type -> type == TableType.VIEW)) {
                MvPlanContext contextWithView = mvOptimizer.optimize(mv, connectContext, false);
                results.add(contextWithView);
            }
        }
        return results;
    }
}
