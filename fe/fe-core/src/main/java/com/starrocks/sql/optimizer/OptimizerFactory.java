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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;

public class OptimizerFactory {
    @VisibleForTesting
    public static OptimizerContext mockContext(ConnectContext context, ColumnRefFactory columnRefFactory,
                                               OptimizerOptions config) {
        OptimizerContext oc = new OptimizerContext(context);
        oc.setColumnRefFactory(columnRefFactory);
        oc.setOptimizerOptions(config);
        return oc;
    }

    @VisibleForTesting
    public static OptimizerContext mockContext(ConnectContext context, ColumnRefFactory columnRefFactory) {
        return mockContext(context, columnRefFactory, OptimizerOptions.defaultOpt());
    }

    @VisibleForTesting
    public static OptimizerContext mockContext(ColumnRefFactory columnRefFactory) {
        OptimizerContext oc = new OptimizerContext(new ConnectContext());
        oc.setColumnRefFactory(columnRefFactory);
        oc.setOptimizerOptions(OptimizerOptions.defaultOpt());
        return oc;
    }

    public static OptimizerContext initContext(ConnectContext context, ColumnRefFactory columnRefFactory,
                                               OptimizerOptions config) {
        OptimizerContext oc = new OptimizerContext(context);
        oc.setColumnRefFactory(columnRefFactory);
        oc.setOptimizerOptions(config);
        return oc;
    }

    public static OptimizerContext initContext(ConnectContext context, ColumnRefFactory columnRefFactory) {
        return initContext(context, columnRefFactory, OptimizerOptions.defaultOpt());
    }

    public static Optimizer create(OptimizerContext context) {
        if (context.getOptimizerOptions().isShortCircuit()) {
            return new ShortCircuitOptimizer(context);
        } else if (context.getOptimizerOptions().isBaselinePlan()) {
            return new SPMOptimizer(context);
        }
        return new QueryOptimizer(context);
    }
}
