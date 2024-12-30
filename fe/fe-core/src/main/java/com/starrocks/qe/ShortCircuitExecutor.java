// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.google.common.base.Preconditions;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

public class ShortCircuitExecutor {

    protected final ConnectContext context;

    protected final PlanFragment planFragment;

    protected final List<TScanRangeLocations> scanRangeLocations;

    protected final TDescriptorTable tDescriptorTable;

    protected final boolean isBinaryRow;

    protected final boolean enableProfile;

    protected final String protocol;

    protected ShortCircuitResult result = null;

    protected Map<String, RuntimeProfile> perBeExecutionProfile;

    protected final WorkerProvider workerProvider;

    protected ShortCircuitExecutor(ConnectContext context, PlanFragment planFragment,
                                   List<TScanRangeLocations> scanRangeLocations, TDescriptorTable tDescriptorTable,
                                   boolean isBinaryRow,
                                   boolean enableProfile, String protocol, WorkerProvider workerProvider) {
        this.context = context;
        this.planFragment = planFragment;
        this.scanRangeLocations = scanRangeLocations;
        this.tDescriptorTable = tDescriptorTable;
        this.isBinaryRow = isBinaryRow;
        this.enableProfile = enableProfile;
        this.protocol = protocol;
        if (enableProfile) {
            this.perBeExecutionProfile = new HashMap<>();
        } else {
            this.perBeExecutionProfile = Collections.emptyMap();
        }
        this.workerProvider = workerProvider;
    }

    public static ShortCircuitExecutor create(ConnectContext context, List<PlanFragment> fragments, List<ScanNode> scanNodes,
                                              TDescriptorTable tDescriptorTable, boolean isBinaryRow, boolean enableProfile,
                                              String protocol,
                                              WorkerProvider workerProvider) {
        if (fragments.size() != 1 || !fragments.get(0).isShortCircuit()) {
            return null;
        }

        boolean isEmpty = scanNodes.isEmpty();
        if (!isEmpty && scanNodes.get(0) instanceof OlapScanNode) {
            List<TScanRangeLocations> scanRangeLocations = scanNodes.get(0).getScanRangeLocations(0);
            return new ShortCircuitHybridExecutor(context, fragments.get(0), scanRangeLocations, tDescriptorTable, isBinaryRow,
                    enableProfile, protocol, workerProvider);
        }
        return null;
    }

    public void exec() throws StarRocksException, StarRocksPlannerException {
        throw new StarRocksPlannerException("Not implement ShortCircuit Executor class", ErrorType.INTERNAL_ERROR);
    }

    protected static Optional<Backend> pick(Set<Long> backendId, Map<Long, Backend> aliveBackends) {
        for (Long beId : backendId) {
            Optional<Backend> backend = Optional.ofNullable(aliveBackends.get(beId));
            if (backend.isPresent()) {
                return backend;
            }
        }

        return Optional.empty();
    }

    public RowBatch getNext() {
        Preconditions.checkNotNull(result);
        return result.getRowBatches().size() == 0 ? new RowBatch() : result.getRowBatches().poll();
    }

    public RuntimeProfile buildQueryProfile(boolean needMerge) {
        RuntimeProfile executionProfile = new RuntimeProfile("Short Circuit Executor");
        perBeExecutionProfile.forEach((key, beProfile) -> {
            // compute localTimePercent for each backend profile
            beProfile.computeTimeInProfile();
            executionProfile.addChild(beProfile);
        });
        return executionProfile;
    }

    public Optional<RuntimeProfile> getRuntimeProfile() {
        Preconditions.checkNotNull(result);
        return result.getRuntimeProfile();
    }

    public static class ShortCircuitResult {
        private final Queue<RowBatch> rowBatches;
        private final long affectedRows;
        private final RuntimeProfile runtimeProfile;
        private final String resultInfo;

        public ShortCircuitResult(Queue<RowBatch> rowBatches, long affectedRows, RuntimeProfile runtimeProfile) {
            this.rowBatches = rowBatches;
            this.affectedRows = affectedRows;
            this.runtimeProfile = runtimeProfile;
            this.resultInfo = null;
        }

        public Queue<RowBatch> getRowBatches() {
            return rowBatches;
        }

        public long getAffectedRows() {
            return affectedRows;
        }

        public Optional<RuntimeProfile> getRuntimeProfile() {
            return Optional.ofNullable(runtimeProfile);
        }

        public String getResultInfo() {
            return resultInfo;
        }
    }
}