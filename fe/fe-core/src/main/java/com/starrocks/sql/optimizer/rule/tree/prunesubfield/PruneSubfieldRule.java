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

package com.starrocks.sql.optimizer.rule.tree.prunesubfield;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.rule.tree.TreeRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;

public class PruneSubfieldRule implements TreeRewriteRule {
    public static final List<String> SUPPORT_FUNCTIONS = ImmutableList.<String>builder()
            .add(FunctionSet.MAP_KEYS, FunctionSet.MAP_SIZE)
            .add(FunctionSet.ARRAY_LENGTH)
            .add(FunctionSet.CARDINALITY)
            .build();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        SubfieldAccessPathComputer computer = new SubfieldAccessPathComputer();
        return root.getOp().accept(computer, root, null);
    }
}
