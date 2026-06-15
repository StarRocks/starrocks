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
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.SqlUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Data-tier {@link SampleSubqueryExecutor} for the INSERT-from-OLAP-table path.
 * Synthesizes a {@code SELECT <sort_key_cols>[, <partition_source_cols>] FROM
 * <source_table> [WHERE (<user_pred>) AND] rand(...) < rate ORDER BY rand(...)
 * LIMIT N} sub-query and decodes the JSON result rows using the TARGET column
 * types supplied by the {@link SampleRequest}.
 *
 * <p>Source column names from the {@link InsertFromTableScanContext} drive the
 * SELECT projection so the query targets the right columns in the source table;
 * the corresponding TARGET {@link com.starrocks.catalog.Column} objects from
 * the request drive the JSON decode so each cell is coerced to the destination
 * schema type.
 */
final class InsertFromTableSampleSubqueryExecutor extends AbstractSqlSampleSubqueryExecutor {

    private static final String ERROR_PREFIX = "INSERT-from-table data tier ";

    InsertFromTableSampleSubqueryExecutor() {
        super(ERROR_PREFIX);
    }

    @VisibleForTesting
    InsertFromTableSampleSubqueryExecutor(SampleQueryRunner sampleQueryRunner) {
        super(ERROR_PREFIX, sampleQueryRunner);
    }

    @Override
    protected SampleSpec resolveSampleSpec(SampleRequest request) throws StarRocksException {
        ScanContext scanContext = request.getScanContext();
        if (!(scanContext instanceof InsertFromTableScanContext context)) {
            throw new StarRocksException(ERROR_PREFIX + "received a "
                    + scanContext.getClass().getSimpleName()
                    + " — wire only the INSERT-from-table load kind here");
        }
        return new SampleSpec(
                context.sourceFromSql(),
                context.wherePredicateSql(),
                Math.max(0L, context.sourceTable().getDataSize()),
                context.computeResource(),
                identsOf(context.sortKeySourceColumnNames()),
                identsOf(context.partitionSourceColumnNames()),
                request.getSortKey(),
                request.getPartitionSourceColumns());
    }

    /**
     * Converts a list of raw column names into backtick-quoted SQL identifiers
     * suitable for embedding in a SELECT projection.
     */
    private static List<String> identsOf(List<String> columnNames) {
        return columnNames.stream().map(SqlUtils::getIdentSql).collect(Collectors.toList());
    }
}
