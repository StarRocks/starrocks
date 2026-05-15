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

import java.util.Objects;

/**
 * {@link ScanContext} concrete for the INSERT-from-FILES integration. The
 * sampler executors that consume this context (Tier 1 row-group statistics
 * provider, Tier 2 sub-query executor) build their own {@code FileScanNode}
 * from the {@link TableFunctionTable} and the {@link ComputeResource}; the
 * pipeline itself does not introspect.
 *
 * <p>This context is built BEFORE the load's {@code ExecPlan} exists, so it
 * carries the connector inputs directly rather than the planner's already-
 * built {@code FileScanNode}.
 */
public record InsertFromFilesScanContext(
        TableFunctionTable sourceTable,
        ComputeResource computeResource) implements ScanContext {

    public InsertFromFilesScanContext {
        Objects.requireNonNull(sourceTable, "sourceTable");
        Objects.requireNonNull(computeResource, "computeResource");
    }
}
