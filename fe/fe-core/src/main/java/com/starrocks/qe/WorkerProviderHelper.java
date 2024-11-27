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

package com.starrocks.qe;

import com.google.common.collect.ImmutableMap;
import com.starrocks.system.ComputeNode;

import java.util.function.IntSupplier;

public class WorkerProviderHelper {
    public static <C extends ComputeNode> C getNextWorker(ImmutableMap<Long, C> workers,
                                                          IntSupplier getNextWorkerNodeIndex) {
        if (workers.isEmpty()) {
            return null;
        }
        int index = getNextWorkerNodeIndex.getAsInt() % workers.size();
        if (index < 0) {
            index = -index;
        }
        return workers.values().asList().get(index);
    }
}
