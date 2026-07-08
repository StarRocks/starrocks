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

package com.starrocks.alter.reshard.presplit;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.catalog.Column;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.SqlUtils;

import java.util.List;

/**
 * {@link SampleSubqueryExecutor} for sampling a single partition of an
 * internal (OLAP) table with a new sort key. Synthesizes a
 * {@code SELECT <sort_key_cols>[, <partition_source_cols>] FROM `db`.`tbl`
 * PARTITION (`part`) WHERE rand(...) < rate ORDER BY rand(...) LIMIT N}
 * sub-query and decodes the JSON result rows using the target column types
 * supplied by the {@link SampleRequest}.
 *
 * <p>Source column names from the {@link InternalPartitionScanContext} drive the
 * SELECT projection; the corresponding target {@link Column} objects from the
 * request drive the JSON decode so each cell is coerced to the destination
 * schema type. The sampling rate and row limit are computed by the inherited
 * helpers in {@link AbstractSqlSampleSubqueryExecutor}.
 */
final class InternalPartitionSampleSubqueryExecutor extends AbstractSqlSampleSubqueryExecutor {

    private static final String ERROR_PREFIX = "internal-partition ";

    InternalPartitionSampleSubqueryExecutor() {
        super(ERROR_PREFIX, "TabletPreSplitInternalPartitionSubquery");
    }

    @VisibleForTesting
    InternalPartitionSampleSubqueryExecutor(SampleQueryRunner sampleQueryRunner) {
        super(ERROR_PREFIX, sampleQueryRunner);
    }

    @Override
    protected SampleSpec resolveSampleSpec(SampleRequest request) throws StarRocksException {
        ScanContext scanContext = request.getScanContext();
        if (!(scanContext instanceof InternalPartitionScanContext context)) {
            throw new StarRocksException(ERROR_PREFIX + "received a "
                    + scanContext.getClass().getSimpleName()
                    + " — wire only the internal-partition scan context here");
        }
        String fromClauseSql = SqlUtils.getIdentSql(context.dbName())
                + "." + SqlUtils.getIdentSql(context.tableName())
                + " PARTITION (" + SqlUtils.getIdentSql(context.partitionName()) + ")";
        List<Column> sortKeyColumns = request.getSortKey();
        List<Column> partitionSourceColumns = request.getPartitionSourceColumns();
        return new SampleSpec(
                fromClauseSql,
                /*whereClauseSqlOrNull=*/ null,
                context.partitionSizeBytes(),
                context.computeResource(),
                identsOf(context.sortKeySourceColumnNames()),
                identsOf(context.partitionSourceColumnNames()),
                sortKeyColumns,
                partitionSourceColumns);
    }
}
