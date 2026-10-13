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

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.Map;
import java.util.Objects;

/**
 * {@link ScanContext} concrete for the INSERT-from-table integration.
 * Carries the source {@link Table} reference, source snapshot estimates, and the
 * pre-quoted FROM clause SQL, plus the full target-&gt;source
 * column-name map the sampler uses to project any index's sort key (base or rollup) and
 * the partition columns by their source-table column names. The optional WHERE predicate
 * SQL is threaded through verbatim from the INSERT-SELECT statement so the sample covers
 * only the rows the load will actually write. A key column the SELECT feeds with a literal has no
 * source column; it is carried as the literal's SQL, which the sampler projects instead.
 * A key column the SELECT computes from source columns -- a safe computed projection, or a generated
 * column evaluated from the columns its definition reads -- is carried as the expression's SQL, which
 * the sampler evaluates over the source rows. The load time zone keeps external timestamp decoding
 * aligned with the INSERT. {@code sessionSemantics} are the INSERT session's variables that decide
 * how the predicate and those expressions evaluate; the sampler sets them on its own session.
 */
public record InsertFromTableScanContext(
        Table sourceTable,
        String sourceFromSql,                       // "`db`.`tbl` `alias`" or "`db`.`tbl`"
        Map<String, String> targetToSourceColumnNames,   // directly mapped lower-cased target name -> source name
        String wherePredicateSql,                   // nullable
        ComputeResource computeResource,
        long sourceTotalBytes,
        long sourceTotalRows,
        Map<String, String> targetToConstantSql,    // lower-cased target name -> SQL
        Map<String, String> targetToExpressionSql,  // lower-cased target name -> SQL
        String loadTimeZone,
        SampleSessionSemantics sessionSemantics) implements ScanContext {

    public InsertFromTableScanContext {
        Objects.requireNonNull(sourceTable, "sourceTable");
        Objects.requireNonNull(sourceFromSql, "sourceFromSql");
        Objects.requireNonNull(targetToSourceColumnNames, "targetToSourceColumnNames");
        Objects.requireNonNull(computeResource, "computeResource");
        Objects.requireNonNull(targetToConstantSql, "targetToConstantSql");
        Objects.requireNonNull(targetToExpressionSql, "targetToExpressionSql");
        Objects.requireNonNull(sessionSemantics, "sessionSemantics");
        if (sourceTotalBytes < 0 || sourceTotalRows < 0) {
            throw new IllegalArgumentException("source estimates must be non-negative");
        }
    }

    /** Every key column is backed by a source column. */
    public InsertFromTableScanContext(
            Table sourceTable, String sourceFromSql, Map<String, String> targetToSourceColumnNames,
            String wherePredicateSql, ComputeResource computeResource, long sourceTotalBytes, long sourceTotalRows) {
        this(sourceTable, sourceFromSql, targetToSourceColumnNames, wherePredicateSql, computeResource,
                sourceTotalBytes, sourceTotalRows, Map.of(), Map.of(), null, SampleSessionSemantics.NONE);
    }

    /** Backward-compatible constructor for the original internal-OLAP source path and its tests. */
    public InsertFromTableScanContext(
            OlapTable sourceTable, String sourceFromSql, Map<String, String> targetToSourceColumnNames,
            String wherePredicateSql, ComputeResource computeResource) {
        this(sourceTable, sourceFromSql, targetToSourceColumnNames, wherePredicateSql, computeResource,
                Math.max(0L, sourceTable.getDataSize()), Math.max(0L, sourceTable.getRowCount()));
    }
}
