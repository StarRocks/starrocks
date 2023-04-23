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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Table;

import java.util.List;
import java.util.Map;

public class TableScanContext {
    private final List<TableScanDesc> tableScanDescs;
    private final Map<Table, Integer> tableIdMap;

    public TableScanContext() {
        this.tableScanDescs = Lists.newArrayList();
        this.tableIdMap = Maps.newHashMap();
    }

    public List<TableScanDesc> getTableScanDescs() {
        return tableScanDescs;
    }

    public Map<Table, Integer> getTableIdMap() {
        return tableIdMap;
    }
}
