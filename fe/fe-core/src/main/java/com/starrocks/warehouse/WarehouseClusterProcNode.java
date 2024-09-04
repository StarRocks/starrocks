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

package com.starrocks.warehouse;

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/warehouse/WarehouseClusterProcNode.java
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;

public class WarehouseClusterProcNode implements ProcNodeInterface {
    private final Warehouse warehouse;

    public WarehouseClusterProcNode(Warehouse wh) {
        this.warehouse = wh;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        return warehouse.getClusterProcData();
=======
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.task.AgentTaskQueue;

import java.util.List;
import java.util.Map;

public class AgentTaskTracker implements MemoryTrackable {
    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("AgentTask", (long) AgentTaskQueue.getTaskNum());
>>>>>>> f0cb5e97c8 ([Enhancement] Optimize memory tracker (#49841)):fe/fe-core/src/main/java/com/starrocks/memory/AgentTaskTracker.java
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        return Lists.newArrayList(Pair.create(AgentTaskQueue.getSamplesForMemoryTracker(),
                (long) AgentTaskQueue.getTaskNum()));
    }
}
