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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
                Math.max(0L, context.sourceTable().getDataSize()),
                context.computeResource(),
                identsOf(mapToSource(request.getSortKey(), context.targetToSourceColumnNames())),
                identsOf(mapToSource(request.getPartitionSourceColumns(), context.targetToSourceColumnNames())),
                request.getSortKey(),
                request.getPartitionSourceColumns());
    }

    /**
     * Projects each rollup's sort key by remapping every column to its source-table name via the
     * scan context's target-&gt;source map. The base sort key is projected the same way in
     * {@link #resolveSampleSpec}; both go through {@link #mapToSource}, so a rollup whose sort key
     * reorders or subsets the base is projected correctly regardless of source column naming.
     */
    @Override
    protected List<String> secondaryProjectionIdents(SampleRequest request) throws StarRocksException {
        InsertFromTableScanContext context = contextOf(request);
        List<String> idents = new ArrayList<>();
        for (SecondaryIndexSpec spec : request.getSecondaryIndexSortKeys()) {
            idents.addAll(identsOf(mapToSource(spec.sortKey(), context.targetToSourceColumnNames())));
        }
        return idents;
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
     * Remaps target columns (a sort key -- base or rollup -- or the partition columns) to their
     * source-table column names via {@link InsertSelectSourceColumns#lookup}. Throws (-&gt; the
     * sample fails -&gt; the load proceeds without pre-split) if any column is unmapped, so a
     * boundary is never computed against the wrong source column. {@code prepare} gates this at
     * admission time; the throw remains as the fail-safe for a metadata race between prepare and
     * sampling.
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
