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

import java.util.List;
import java.util.Objects;

/**
 * {@link ScanContext} concrete for the INSERT-from-table integration.
 * Carries the source {@link Table} reference and its snapshot estimates so the sampler can size
 * the input, the pre-quoted FROM clause SQL, plus the source-column name lists that map the
 * target sort key and partition columns back to their source equivalents. The optional WHERE
 * predicate SQL is threaded through verbatim from the INSERT-SELECT statement so the sample
 * covers only the rows the load will actually write.
 *
 * <p>The estimates are carried explicitly rather than read back off {@code sourceTable} because an
 * external source does not expose them the way an {@link OlapTable} does: an Iceberg table's totals
 * come from its current snapshot summary, which the resolver reads once.
 */
public record InsertFromTableScanContext(
        Table sourceTable,
        String sourceFromSql,                       // "`db`.`tbl` `alias`" or "`catalog`.`db`.`tbl` `alias`"
        List<String> sortKeySourceColumnNames,
        List<String> partitionSourceColumnNames,
        String wherePredicateSql,                   // nullable
        ComputeResource computeResource,
        long sourceTotalBytes,
        long sourceTotalRows) implements ScanContext {

    public InsertFromTableScanContext {
        Objects.requireNonNull(sourceTable, "sourceTable");
        Objects.requireNonNull(sourceFromSql, "sourceFromSql");
        Objects.requireNonNull(sortKeySourceColumnNames, "sortKeySourceColumnNames");
        Objects.requireNonNull(partitionSourceColumnNames, "partitionSourceColumnNames");
        Objects.requireNonNull(computeResource, "computeResource");
        if (sourceTotalBytes < 0 || sourceTotalRows < 0) {
            throw new IllegalArgumentException("source estimates must be non-negative");
        }
    }

    /** Backward-compatible constructor for the original internal-OLAP source path and its tests. */
    public InsertFromTableScanContext(
            OlapTable sourceTable, String sourceFromSql, List<String> sortKeySourceColumnNames,
            List<String> partitionSourceColumnNames, String wherePredicateSql,
            ComputeResource computeResource) {
        this(sourceTable, sourceFromSql, sortKeySourceColumnNames, partitionSourceColumnNames,
                wherePredicateSql, computeResource,
                Math.max(0L, sourceTable.getDataSize()), Math.max(0L, sourceTable.getRowCount()));
    }
}
