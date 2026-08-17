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

package com.starrocks.connector;

/**
 * Tracks which resolution strategy was used to map an external base partition
 * to MV partition key(s). Useful for debugging and future multi-spec dispatch.
 */
public enum PartitionKeyResolutionPath {
    /**
     * Direct extraction from partition name values (Hive, Hudi, Paimon, etc.)
     */
    DIRECT_NAME_VALUE,

    /**
     * JDBC-specific extraction with MAXVALUE handling.
     */
    JDBC_NAME_VALUE,

    /**
     * Iceberg current partition spec direct mapping.
     */
    ICEBERG_CURRENT_SPEC,

    // Future phases:
    // ICEBERG_HISTORICAL_SPEC,
    // ICEBERG_PARTITION_EXPR_FALLBACK,
    // ICEBERG_SYNTHETIC_TRANSFORM,
}
