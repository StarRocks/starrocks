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
 * Carries the source {@link Table} reference and its snapshot estimates so the sampler can size
 * the input, the pre-quoted FROM clause SQL, plus the target-&gt;source column-name map the sampler
 * uses to project the sort key and the partition columns by their source column names. The optional WHERE
 * predicate SQL is threaded through verbatim from the INSERT-SELECT statement so the sample
 * covers only the rows the load will actually write. A partition column the SELECT feeds with a literal
 * has no source column; it is carried as the literal's SQL, which the sampler projects instead.
 *
 * <p>The estimates are carried explicitly rather than read back off {@code sourceTable} because an
 * external source does not expose them the way an {@link OlapTable} does: an Iceberg table's totals
 * come from its current snapshot summary, which the resolver reads once.
 */
public record InsertFromTableScanContext(
        Table sourceTable,
        String sourceFromSql,                       // "`db`.`tbl` `alias`" or "`catalog`.`db`.`tbl` `alias`"
        Map<String, String> targetToSourceColumnNames,   // directly mapped lower-cased target name -> source name
        String wherePredicateSql,                   // nullable
        ComputeResource computeResource,
        long sourceTotalBytes,
        long sourceTotalRows,
        Map<String, String> targetToConstantPartitionSql) implements ScanContext {   // lower-cased target name -> SQL

    public InsertFromTableScanContext {
        Objects.requireNonNull(sourceTable, "sourceTable");
        Objects.requireNonNull(sourceFromSql, "sourceFromSql");
        Objects.requireNonNull(targetToSourceColumnNames, "targetToSourceColumnNames");
        Objects.requireNonNull(computeResource, "computeResource");
        Objects.requireNonNull(targetToConstantPartitionSql, "targetToConstantPartitionSql");
        if (sourceTotalBytes < 0 || sourceTotalRows < 0) {
            throw new IllegalArgumentException("source estimates must be non-negative");
        }
    }

    /** Every partition column is backed by a source column. */
    public InsertFromTableScanContext(
            Table sourceTable, String sourceFromSql, Map<String, String> targetToSourceColumnNames,
            String wherePredicateSql, ComputeResource computeResource, long sourceTotalBytes, long sourceTotalRows) {
        this(sourceTable, sourceFromSql, targetToSourceColumnNames, wherePredicateSql, computeResource,
                sourceTotalBytes, sourceTotalRows, Map.of());
    }

    /** Backward-compatible constructor for the original internal-OLAP source path and its tests. */
    public InsertFromTableScanContext(
            OlapTable sourceTable, String sourceFromSql, Map<String, String> targetToSourceColumnNames,
            String wherePredicateSql, ComputeResource computeResource) {
        this(sourceTable, sourceFromSql, targetToSourceColumnNames, wherePredicateSql, computeResource,
                Math.max(0L, sourceTable.getDataSize()), Math.max(0L, sourceTable.getRowCount()));
    }
}
