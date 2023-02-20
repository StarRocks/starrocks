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


package com.starrocks.connector;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

// Loading cache in CachingHiveMetastore may trigger loading of another cache entry for different key type.
// If there are no empty executor slots, such operation would deadlock. ReentrantExecutor is necessary.
public class ReentrantExecutor implements Executor {
    private final ThreadLocal<Boolean> executorMarker = ThreadLocal.withInitial(() -> false);
    private final Executor boundedExecutor;
    private final Executor coreExecutor;

    public ReentrantExecutor(Executor coreExecutor, int maxThreads) {
        this.boundedExecutor = new BoundedExecutor(requireNonNull(coreExecutor, "coreExecutor is null"), maxThreads);
        this.coreExecutor = coreExecutor;
    }

    @Override
    public void execute(@NotNull Runnable task) {
        if (executorMarker.get()) {
            coreExecutor.execute(task);
            return;
        }

        boundedExecutor.execute(() -> {
            executorMarker.set(true);
            try {
                task.run();
            } finally {
                executorMarker.remove();
            }
        });
    }
}
