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
import com.google.common.collect.ImmutableList;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;

public class ShortCircuitExecutor {

    protected final ConnectContext context;

    protected final PlanFragment planFragment;

    protected final List<TScanRangeLocations> scanRangeLocations;

    protected final TDescriptorTable tDescriptorTable;

    protected final boolean isBinaryRow;

    protected final boolean enableProfile;

    protected ShortCircuitResult result = null;

    private static final Random RANDOM = new Random(); // NOSONAR

    protected ShortCircuitExecutor(ConnectContext context, PlanFragment planFragment,
                                   List<TScanRangeLocations> scanRangeLocations, TDescriptorTable tDescriptorTable,
                                   boolean isBinaryRow, boolean enableProfile) {
        this.context = context;
        this.planFragment = planFragment;
        this.scanRangeLocations = scanRangeLocations;
        this.tDescriptorTable = tDescriptorTable;
        this.isBinaryRow = isBinaryRow;
        this.enableProfile = enableProfile;
    }

    public static ShortCircuitExecutor create(ConnectContext context, List<PlanFragment> fragments,
                                              List<ScanNode> scanNodes, TDescriptorTable tDescriptorTable,
                                              boolean isBinaryRow, boolean enableProfile) {
        boolean isEmpty = scanNodes.isEmpty();
        List<TScanRangeLocations> scanRangeLocations = isEmpty ?
                ImmutableList.of() : scanNodes.get(0).getScanRangeLocations(0);
        if (fragments.size() != 1 || !fragments.get(0).isShortCircuit()) {
            return null;
        }

        if (!isEmpty && scanNodes.get(0) instanceof OlapScanNode) {
            return new ShortCircuitHybridExecutor(context, fragments.get(0), scanRangeLocations,
                    tDescriptorTable, isBinaryRow, enableProfile);
        }
        return null;
    }

    public void exec() {
        throw new StarRocksPlannerException("Not implement ShortCircuit Executor class", ErrorType.INTERNAL_ERROR);
    }

    protected static <T> T pick(List<T> collections) {
        Preconditions.checkArgument(!collections.isEmpty());
        if (collections.size() == 1) {
            return collections.get(0);
        } else {
            return collections.get(RANDOM.nextInt(collections.size()));
        }
    }

    public RowBatch getNext() {
        Preconditions.checkNotNull(result);
        return result.getRowBatches().poll();
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