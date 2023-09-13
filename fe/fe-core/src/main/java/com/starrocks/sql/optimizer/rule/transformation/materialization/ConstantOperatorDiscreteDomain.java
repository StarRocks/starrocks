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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.DiscreteDomain;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.jetbrains.annotations.Nullable;

public class ConstantOperatorDiscreteDomain extends DiscreteDomain<ConstantOperator> {
    @Nullable
    @Override
    public ConstantOperator next(ConstantOperator value) {
        return value.successor().orElse(null);
    }

    @Nullable
    @Override
    public ConstantOperator previous(ConstantOperator value) {
        return value.predecessor().orElse(null);
    }

    @Override
    public long distance(ConstantOperator start, ConstantOperator end) {
        return start.distance(end);
    }

    public static boolean isSupportedType(Type type) {
        return type.isIntegerType() || type.isLargeint() || type.isDateType();
    }
}
