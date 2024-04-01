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

package com.starrocks.memory;

import com.google.common.collect.ImmutableMap;
import com.starrocks.task.AgentTaskQueue;
import org.apache.spark.util.SizeEstimator;

import java.util.Map;

public class AgentTaskTracker implements MemoryTrackable {
    @Override
    public long estimateSize() {
        return SizeEstimator.estimate(AgentTaskQueue.tasks);
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("AgentTask", (long) AgentTaskQueue.getTaskNum());
    }
}
