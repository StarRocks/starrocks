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

package com.starrocks.qe.scheduler;

import com.google.common.collect.ImmutableList;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Restricts a delegate {@link WorkerProvider} to an allowed worker id set (L ∪ N) and exposes
 * preferred connector workers (L) for connector scan assignment.
 */
public class LocationRestrictedWorkerProvider implements WorkerProvider {
    private final WorkerProvider delegate;
    private final Set<Long> allowedWorkerIds;
    private final Set<Long> preferredConnectorWorkerIds;
    private final AtomicInteger nextIndex = new AtomicInteger(0);
    private final List<ComputeNode> allowedWorkers;
    private final List<ComputeNode> preferredConnectorWorkers;

    public LocationRestrictedWorkerProvider(WorkerProvider delegate,
                                            Set<Long> allowedWorkerIds,
                                            Set<Long> preferredConnectorWorkerIds) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.allowedWorkerIds = Objects.requireNonNull(allowedWorkerIds, "allowedWorkerIds");
        this.preferredConnectorWorkerIds = Objects.requireNonNull(preferredConnectorWorkerIds,
                "preferredConnectorWorkerIds");

        // Resolve allowed workers from the delegate so we keep the same alive/blacklist view.
        List<ComputeNode> allowed = new ArrayList<>();
        for (Long id : allowedWorkerIds) {
            ComputeNode worker = delegate.getWorkerById(id);
            if (worker != null) {
                allowed.add(worker);
            }
        }
        // Also include any already-visible workers from getAllWorkers that are allowed
        // (covers cases where getWorkerById misses CN-only maps while getAllWorkers has them).
        for (ComputeNode worker : delegate.getAllWorkers()) {
            if (allowedWorkerIds.contains(worker.getId())
                    && allowed.stream().noneMatch(w -> w.getId() == worker.getId())) {
                allowed.add(worker);
            }
        }
        this.allowedWorkers = ImmutableList.copyOf(allowed);

        List<ComputeNode> preferred = allowedWorkers.stream()
                .filter(w -> preferredConnectorWorkerIds.contains(w.getId()))
                .collect(Collectors.toList());
        // Prefer labelled backends even when prefer_compute_node made getAllWorkers return CNs:
        // look them up individually from the delegate.
        if (preferred.isEmpty()) {
            for (Long id : preferredConnectorWorkerIds) {
                ComputeNode worker = delegate.getWorkerById(id);
                if (worker != null) {
                    preferred.add(worker);
                }
            }
        }
        this.preferredConnectorWorkers = ImmutableList.copyOf(preferred);
    }

    public static WorkerProvider maybeWrap(WorkerProvider delegate,
                                           ConnectorLocationWorkerRestrictor.Restriction restriction) {
        if (restriction == null || !restriction.isRestricted()) {
            return delegate;
        }
        return new LocationRestrictedWorkerProvider(delegate,
                restriction.getAllowedWorkerIds(),
                restriction.getPreferredConnectorWorkerIds());
    }

    @Override
    public long selectNextWorker() throws NonRecoverableException {
        if (allowedWorkers.isEmpty()) {
            reportWorkerNotFoundException();
        }
        int index = Math.floorMod(nextIndex.getAndIncrement(), allowedWorkers.size());
        ComputeNode worker = allowedWorkers.get(index);
        selectWorkerUnchecked(worker.getId());
        return worker.getId();
    }

    @Override
    public void selectWorker(long workerId) throws NonRecoverableException {
        if (!allowedWorkerIds.contains(workerId) || getWorkerById(workerId) == null) {
            reportWorkerNotFoundException();
        }
        delegate.selectWorkerUnchecked(workerId);
    }

    @Override
    public List<Long> selectAllComputeNodes() {
        return delegate.selectAllComputeNodes().stream()
                .filter(allowedWorkerIds::contains)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<ComputeNode> getAllWorkers() {
        return allowedWorkers;
    }

    /**
     * Prefer L for connector scan assignment. Soft-fallback: if no preferred workers are
     * visible on this provider (e.g. labelled BEs missing from the delegate view after
     * {@code prefer_compute_node} filtering), return {@code allowedWorkers} (L∪N) so
     * connector splits can still be placed within the restricted pool instead of failing
     * with an empty worker set. That may place connector scans on native replica BEs (N).
     */
    @Override
    public Collection<ComputeNode> getPreferredConnectorWorkers() {
        return preferredConnectorWorkers.isEmpty() ? allowedWorkers : preferredConnectorWorkers;
    }

    @Override
    public ComputeNode getWorkerById(long workerId) {
        if (!allowedWorkerIds.contains(workerId)) {
            return null;
        }
        return delegate.getWorkerById(workerId);
    }

    @Override
    public boolean isDataNodeAvailable(long dataNodeId) {
        return allowedWorkerIds.contains(dataNodeId) && delegate.isDataNodeAvailable(dataNodeId);
    }

    @Override
    public void reportDataNodeNotFoundException() throws NonRecoverableException {
        delegate.reportDataNodeNotFoundException();
    }

    @Override
    public void reportWorkerNotFoundException(String errorMessagePrefix) throws NonRecoverableException {
        delegate.reportWorkerNotFoundException(errorMessagePrefix);
    }

    @Override
    public boolean isWorkerSelected(long workerId) {
        return delegate.isWorkerSelected(workerId);
    }

    @Override
    public List<Long> getSelectedWorkerIds() {
        return delegate.getSelectedWorkerIds();
    }

    @Override
    public List<Long> getAllAvailableNodes() {
        return delegate.getAllAvailableNodes().stream()
                .filter(allowedWorkerIds::contains)
                .collect(Collectors.toList());
    }

    @Override
    public void selectWorkerUnchecked(long workerId) {
        delegate.selectWorkerUnchecked(workerId);
    }

    @Override
    public boolean isPreferComputeNode() {
        // Restriction forces labelled backends for connector/shuffle locality.
        return false;
    }

    @Override
    public boolean allowUsingBackupNode() {
        return delegate.allowUsingBackupNode();
    }

    @Override
    public long selectBackupWorker(long workerId) {
        long backup = delegate.selectBackupWorker(workerId);
        return allowedWorkerIds.contains(backup) ? backup : -1;
    }

    @Override
    public long selectBackupWorker(long workerId, long affinityKey) {
        long backup = delegate.selectBackupWorker(workerId, affinityKey);
        return allowedWorkerIds.contains(backup) ? backup : -1;
    }

    @Override
    public ComputeResource getComputeResource() {
        return delegate.getComputeResource();
    }
}
