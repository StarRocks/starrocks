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

package com.starrocks.sql.automv.pn;

import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.util.TieredMap;

import java.util.Objects;

public class ColumnsAndSubstMap {
    private final TieredMap<Integer, GenericColumn> columns;
    private final TieredMap<Integer, Op> substMap;

    private ColumnsAndSubstMap(TieredMap<Integer, GenericColumn> columns, TieredMap<Integer, Op> substMap) {
        this.columns = Objects.requireNonNull(columns);
        this.substMap = Objects.requireNonNull(substMap);
    }

    public static ColumnsAndSubstMap of(TieredMap<Integer, GenericColumn> columns, TieredMap<Integer, Op> substMap) {
        return new ColumnsAndSubstMap(columns, substMap);
    }

    public TieredMap<Integer, GenericColumn> getColumns() {
        return columns;
    }

    public TieredMap<Integer, Op> getSubstMap() {
        return substMap;
    }
}
