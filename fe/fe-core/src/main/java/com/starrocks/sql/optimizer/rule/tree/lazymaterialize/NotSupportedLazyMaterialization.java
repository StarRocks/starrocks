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

package com.starrocks.sql.optimizer.rule.tree.lazymaterialize;

import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;

public class NotSupportedLazyMaterialization implements LazyMaterializationSupport {
    @Override
    public boolean supports(PhysicalScanOperator scanOperator) {
        return false;
    }

    @Override
    public ColumnRefSet predicateUsedColumns(PhysicalScanOperator scanOperator) {
        return null;
    }

    @Override
    public List<ColumnRefOperator> addRowIdColumns(PhysicalScanOperator scanOperator,
                                                   ColumnRefFactory columnRefFactory) {
        return List.of();
    }

    @Override
    public OptExpression updateOutputColumns(OptExpression scan,
                                             Map<ColumnRefOperator, Column> columnRef2Column) {
        return null;
    }

}
