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

import com.starrocks.load.BrokerFileGroup;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.List;
import java.util.Objects;

/**
 * {@link ScanContext} concrete for the Broker Load integration. The sampler
 * executors that consume this context (Tier 1 row-group statistics provider,
 * Tier 2 sub-query executor) build their own {@code FileScanNode} from the
 * {@link BrokerDesc}, the {@link BrokerFileGroup} list, and the
 * {@link ComputeResource}; the pipeline itself does not introspect.
 *
 * <p>{@code fileStatusesPerGroup} carries the file-status snapshot the load
 * pending task already resolved, parallel to {@link #fileGroups()}. Tier 1
 * reads exactly this snapshot rather than re-globbing — re-listing would
 * race with the load's own enumeration and risk planning quantile cuts
 * from a different file set than the one actually loaded.
 *
 * <p>The {@code brokerDesc} may be {@code null} for HDFS-style direct loads
 * that don't go through a broker — matching the same nullability rule the
 * load planner already follows.
 */
public record BrokerLoadScanContext(
        BrokerDesc brokerDesc,
        List<BrokerFileGroup> fileGroups,
        List<List<TBrokerFileStatus>> fileStatusesPerGroup,
        ComputeResource computeResource) implements ScanContext {

    public BrokerLoadScanContext {
        Objects.requireNonNull(fileGroups, "fileGroups");
        Objects.requireNonNull(fileStatusesPerGroup, "fileStatusesPerGroup");
        Objects.requireNonNull(computeResource, "computeResource");
        if (fileGroups.size() != fileStatusesPerGroup.size()) {
            throw new IllegalArgumentException(String.format(
                    "fileGroups size %d != fileStatusesPerGroup size %d",
                    fileGroups.size(), fileStatusesPerGroup.size()));
        }
    }
}
