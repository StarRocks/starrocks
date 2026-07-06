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

import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.List;
import java.util.Objects;

/**
 * {@link ScanContext} for sampling a single partition of an internal (OLAP) table.
 * Carries the database name, table name, and partition name that compose the
 * {@code FROM <db>.<table> PARTITION(<partition>)} clause, the source column names
 * that map the new sort key and partition-source columns back to their positions in
 * the source table, and the partition's data size estimate used to derive the
 * Bernoulli sampling rate.
 */
public record InternalPartitionScanContext(
        String dbName,
        String tableName,
        String partitionName,
        List<String> sortKeySourceColumnNames,
        List<String> partitionSourceColumnNames,
        long partitionSizeBytes,
        ComputeResource computeResource) implements ScanContext {

    public InternalPartitionScanContext {
        Objects.requireNonNull(dbName, "dbName");
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(partitionName, "partitionName");
        Objects.requireNonNull(sortKeySourceColumnNames, "sortKeySourceColumnNames");
        Objects.requireNonNull(partitionSourceColumnNames, "partitionSourceColumnNames");
        Objects.requireNonNull(computeResource, "computeResource");
        if (partitionSizeBytes < 0) {
            throw new IllegalArgumentException("partitionSizeBytes must be non-negative, was " + partitionSizeBytes);
        }
    }
}
