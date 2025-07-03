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

package com.starrocks.epack.warehouse.cngroup;

import com.starrocks.qe.GlobalVariable;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.List;
import java.util.Optional;

public class CNGroupResourceUsage implements Comparable<CNGroupResourceUsage> {
    private final CNGroupResource cnGroupResource;

    private final long aliveComputeNodeCount;
    private final long maxRunningQueries;
    private final double avgCpuUsedPermille;
    private final long freshComputeNodeCount;

    public CNGroupResourceUsage(CNGroupResource cnGroupResource,
                                List<ComputeNode> computeNodes) {
        this.cnGroupResource = cnGroupResource;
        this.aliveComputeNodeCount = computeNodes == null ? 0 : computeNodes.size();
        double sumCpuUsedPermille = 0.0;
        int maxRunningQueries = 0;
        int freshComputeNodeCount = 0;
        for (int i = 0; i < this.aliveComputeNodeCount; i++) {
            ComputeNode computeNode = computeNodes.get(i);
            if (!computeNode.isResourceUsageFresh()) {
                continue;
            }
            sumCpuUsedPermille += computeNode.getCpuUsedPermille();
            maxRunningQueries = Math.max(maxRunningQueries, computeNode.getNumRunningQueries());
            freshComputeNodeCount++;
        }
        this.freshComputeNodeCount = freshComputeNodeCount;
        this.avgCpuUsedPermille = freshComputeNodeCount == 0 ? Double.MAX_VALUE
                : sumCpuUsedPermille / freshComputeNodeCount;
        this.maxRunningQueries = freshComputeNodeCount == 0 ? Long.MAX_VALUE :
                maxRunningQueries;
    }

    public CNGroupResourceUsage(CNGroupResource cnGroupResource,
                                long aliveComputeNodeCount,
                                long maxRunningQueries,
                                double avgCpuUsedPermille,
                                long freshComputeNodeCount) {
        this.cnGroupResource = cnGroupResource;
        this.aliveComputeNodeCount = aliveComputeNodeCount;
        this.maxRunningQueries = maxRunningQueries;
        this.avgCpuUsedPermille = avgCpuUsedPermille;
        this.freshComputeNodeCount = freshComputeNodeCount;
    }

    public static CNGroupResourceUsage of(CNGroupResource cnGroupResource, List<ComputeNode> computeNodes) {
        return new CNGroupResourceUsage(cnGroupResource, computeNodes == null ? List.of() : computeNodes);
    }

    public CNGroupResource getCnGroupResource() {
        return cnGroupResource;
    }

    public long getAliveComputeNodeCount() {
        return aliveComputeNodeCount;
    }

    public long getMaxRunningQueries() {
        return maxRunningQueries;
    }

    public double getAvgCpuUsedPermille() {
        return avgCpuUsedPermille;
    }

    public long getFreshComputeNodeCount() {
        return freshComputeNodeCount;
    }

    public boolean isResourceUsageFresh() {
        double resourceUsageFreshRatio = GlobalVariable.getCngroupResourceUsageFreshRatio();
        if (resourceUsageFreshRatio <= 0.0 || resourceUsageFreshRatio > 1.0) {
            return true; // no limit, always fresh
        }
        return aliveComputeNodeCount > 0 && freshComputeNodeCount > 0
                && freshComputeNodeCount >= aliveComputeNodeCount * resourceUsageFreshRatio;
    }

    public boolean isUnderLowWatermark() {
        if (!this.isResourceUsageFresh()) {
            return false;
        }
        if (maxRunningQueries > GlobalVariable.getCngroupLowWatermarkRunningQueryCount()) {
            return false;
        }
        if (avgCpuUsedPermille > GlobalVariable.getCngroupLowWatermarkCPUUsedPermille()) {
            return false;
        }
        return true;
    }

    private int compareAliveComputeNodeCount(CNGroupResourceUsage other) {
        // less is better
        return Long.compare(this.aliveComputeNodeCount, other.aliveComputeNodeCount);
    }

    private int compareMaxRunningQueries(CNGroupResourceUsage other) {
        // less is better
        if (Math.abs(this.maxRunningQueries - other.maxRunningQueries) < 3) {
            return 0; // treat them equal if the difference is negligible
        }
        return Long.compare(this.maxRunningQueries, other.maxRunningQueries);
    }

    private int compareAvgCpuUsedPermille(CNGroupResourceUsage other) {
        // less is better
        if (Math.abs(this.avgCpuUsedPermille - other.avgCpuUsedPermille) < 30) {
            return 0; // treat them equal if the difference is negligible
        }
        return Double.compare(this.avgCpuUsedPermille, other.avgCpuUsedPermille);
    }

    @Override
    public int compareTo(CNGroupResourceUsage other) {
        // less is better
        int cmp = compareAvgCpuUsedPermille(other);
        if (cmp != 0) {
            return cmp;
        }
        // less is better
        cmp = compareMaxRunningQueries(other);
        if (cmp != 0) {
            return cmp;
        }
        // greater is better
        return compareAliveComputeNodeCount(other);
    }

    @Override
    public String toString() {
        return "{CNGroupResource=" + cnGroupResource +
                ", aliveComputeNodeCount=" + aliveComputeNodeCount +
                ", freshComputeNodeCount=" + freshComputeNodeCount +
                ", avgCpuUsedPermille=" + avgCpuUsedPermille +
                ", maxRunningQueries=" + maxRunningQueries +
                '}';
    }

    public static Optional<ComputeResource> findBestByUsage(List<CNGroupResourceUsage> cnGroupResourceUsages) {
        if (cnGroupResourceUsages == null || cnGroupResourceUsages.isEmpty()) {
            return Optional.empty();
        }
        // sort by aliveComputeNodeCount, maxRunningQueries, avgCpuUsedPermille
        return cnGroupResourceUsages.stream()
                .filter(usage -> usage.getFreshComputeNodeCount() > 0)
                .min(CNGroupResourceUsage::compareTo)
                .map(CNGroupResourceUsage::getCnGroupResource);
    }
}
