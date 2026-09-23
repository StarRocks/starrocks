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

import java.util.List;
import java.util.Map;

/**
 * Data-tier {@link SampleSubqueryExecutor} for the INSERT-from-table path.
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
        super(ERROR_PREFIX, "TabletPreSplitDataTierTableSubquery");
    }

    @VisibleForTesting
    InsertFromTableSampleSubqueryExecutor(SampleQueryRunner sampleQueryRunner) {
        super(ERROR_PREFIX, sampleQueryRunner);
    }

    @Override
    protected SampleSpec resolveSampleSpec(SampleRequest request) throws StarRocksException {
        InsertFromTableScanContext context = contextOf(request);
        return new SampleSpec(
                context.sourceFromSql(),
                context.wherePredicateSql(),
                context.sourceTotalBytes(),
                context.computeResource(),
                identsOf(mapToSource(request.getSortKey(), context.targetToSourceColumnNames())),
                partitionProjections(request.getPartitionSourceColumns(), context),
                request.getSortKey(),
                request.getPartitionSourceColumns(),
                context.sourceTotalRows(),
                context.wherePredicateSql() != null);
    }

    private static InsertFromTableScanContext contextOf(SampleRequest request) throws StarRocksException {
        ScanContext scanContext = request.getScanContext();
        if (!(scanContext instanceof InsertFromTableScanContext context)) {
            throw new StarRocksException(ERROR_PREFIX + "received a "
                    + scanContext.getClass().getSimpleName()
                    + " -- wire only the INSERT-from-table load kind here");
        }
        return context;
    }

    /**
     * Projects each partition column by its source column, or -- when the SELECT feeds it a literal --
     * by that literal cast to the column type. Throws on a column backed by neither, like
     * {@link #mapToSource}.
     */
    private static List<String> partitionProjections(
            List<Column> partitionColumns, InsertFromTableScanContext context) throws StarRocksException {
        List<String> projections = InsertSelectSourceColumns.partitionProjections(
                partitionColumns, context.targetToSourceColumnNames(), context.targetToConstantPartitionSql());
        if (projections == null) {
            throw new StarRocksException(ERROR_PREFIX + "a partition column has neither a source column nor a constant");
        }
        return projections;
    }

    /**
     * Remaps target columns (the sort key or the partition columns) to their source-table column
     * names via {@link InsertSelectSourceColumns#lookup}. Throws (-&gt; the sample fails -&gt; the
     * load proceeds without pre-split) if any column is unmapped, so a boundary is never computed
     * against the wrong source column. {@code prepare} gates this at admission time; the throw
     * remains as the fail-safe for a metadata race between prepare and sampling.
     */
    private static List<String> mapToSource(
            List<Column> targetColumns, Map<String, String> targetToSourceColumnNames) throws StarRocksException {
        List<String> sourceNames = InsertSelectSourceColumns.lookup(targetColumns, targetToSourceColumnNames);
        if (sourceNames == null) {
            throw new StarRocksException(ERROR_PREFIX + "a projected column has no source-table column mapping");
        }
        return sourceNames;
    }
}
