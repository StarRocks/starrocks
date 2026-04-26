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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Introduce lazy loading for WorkerProvider to avoid unnecessary computation and ensure that compute resources
 * are acquired after the query queue scheduling is completed.
 *
 * @param lazy the lazy supplier of WorkerProvider
 */
public record LazyWorkerProvider(Supplier<WorkerProvider> lazy) {
    private static final Logger LOG = LogManager.getLogger(LazyWorkerProvider.class);

    public LazyWorkerProvider(Supplier<WorkerProvider> lazy) {
        this.lazy = Suppliers.memoize(lazy);
    }

    public static LazyWorkerProvider of(Supplier<WorkerProvider> lazy) {
        return new LazyWorkerProvider(lazy);
    }

    public WorkerProvider get() {
        if (LOG.isDebugEnabled()) {
            String queryId = ConnectContext.get() != null ? ConnectContext.get().getQueryId().toString() : "N/A";
            LOG.debug("Materializing WorkerProvider in LazyWorkerProvider, queryId:{}", queryId);
        }
        return lazy.get();
    }
}
