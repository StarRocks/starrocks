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

import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.Map;
import java.util.Objects;

/**
 * {@link ScanContext} concrete for the INSERT-from-FILES integration. The
 * sampler executors that consume this context (meta-tier row-group statistics
 * provider, data-tier sub-query executor) build their own {@code FileScanNode}
 * from the {@link TableFunctionTable} and the {@link ComputeResource}; the
 * pipeline itself does not introspect.
 *
 * <p>This context is built BEFORE the load's {@code ExecPlan} exists, so it
 * carries the connector inputs directly rather than the planner's already-
 * built {@code FileScanNode}.
 *
 * <p>{@code loadTimeZone} is the load session timezone (the same value the BE
 * query globals use). The meta-tier readers use it to reproduce the BE's
 * offset for a UTC-adjusted timestamp; a non-fixed / null zone -> data tier.
 *
 * <p>{@code targetToSourceColumnNames} maps each directly projected target column to the FILES
 * column that backs it, so a statement that names, reorders, or renames its columns is sampled
 * from the same physical column the load writes. An EMPTY map means the projection is
 * name-identity and every consumer may project a target column by its own name.
 *
 * <p>{@code targetToConstantPartitionSql} maps each partition column the SELECT feeds with a literal
 * (lower-cased target name) to the literal's SQL. No file column backs such a column: the data tier
 * projects the literal itself, and the meta tier -- which reads only file footers -- never sees it,
 * because a constant column is never part of an admitted sort key.
 *
 * <p>{@code wherePredicateSql} is the statement's WHERE clause rendered back to SQL, or
 * {@code null}. The data tier copies it into its sampling sub-query; the meta tier cannot apply a
 * predicate to a footer at all and declines the request when one is present.
 */
public record InsertFromFilesScanContext(
        TableFunctionTable sourceTable,
        ComputeResource computeResource,
        String loadTimeZone,
        Map<String, String> targetToSourceColumnNames,   // lower-cased target name -> FILES column name
        String wherePredicateSql,                        // nullable
        Map<String, String> targetToConstantPartitionSql) implements ScanContext {

    public InsertFromFilesScanContext {
        Objects.requireNonNull(sourceTable, "sourceTable");
        Objects.requireNonNull(computeResource, "computeResource");
        Objects.requireNonNull(targetToSourceColumnNames, "targetToSourceColumnNames");
        Objects.requireNonNull(targetToConstantPartitionSql, "targetToConstantPartitionSql");
    }

    /** Every partition column is backed by a FILES column. */
    public InsertFromFilesScanContext(
            TableFunctionTable sourceTable, ComputeResource computeResource, String loadTimeZone,
            Map<String, String> targetToSourceColumnNames, String wherePredicateSql) {
        this(sourceTable, computeResource, loadTimeZone, targetToSourceColumnNames, wherePredicateSql, Map.of());
    }

    /** A name-identity projection with no predicate -- the original bare {@code SELECT *} shape. */
    public InsertFromFilesScanContext(
            TableFunctionTable sourceTable, ComputeResource computeResource, String loadTimeZone) {
        this(sourceTable, computeResource, loadTimeZone, Map.of(), null);
    }
}
