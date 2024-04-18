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

package org.apache.iceberg;

import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.MetricsReporter;

public class StarRocksIcebergTableScan
        extends DataScan<TableScan, FileScanTask, CombinedScanTask> implements TableScan {

    public StarRocksIcebergTableScan(Table table, Schema schema, TableScanContext context) {
        super(table, schema, context);
    }

    public static TableScanContext newTableScanContext(Table table) {
        if (table instanceof BaseTable) {
            MetricsReporter reporter = ((BaseTable) table).reporter();
            return ImmutableTableScanContext.builder().metricsReporter(reporter).build();
        } else {
            return TableScanContext.empty();
        }
    }

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
        return null;
    }

    @Override
    protected TableScan newRefinedScan(Table newTable, Schema newSchema, TableScanContext newContext) {
        return null;
    }

    @Override
    public CloseableIterable<CombinedScanTask> planTasks() {
        return null;
    }
}
