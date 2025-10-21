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
// limitations under the License

package com.starrocks.warehouse.cngroup;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Introduce lazy loading for ComputeResource to avoid unnecessary computation and ensure that compute resources
 * are acquired after the query queue scheduling is completed.
 */
public class LazyComputeResource implements ComputeResource {
    private static final Logger LOG = LogManager.getLogger(LazyComputeResource.class);

    private final long warehouseId;
    private final Supplier<ComputeResource> lazy;
    /**
     * Thread-safe flag to track whether the compute resource has been materialized.
     * Uses AtomicBoolean to ensure proper memory visibility and atomicity across threads.
     */
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private LazyComputeResource(long warehouseId, Supplier<ComputeResource> lazy) {
        this.warehouseId = warehouseId;
        this.lazy = Suppliers.memoize(lazy);
    }

    public static LazyComputeResource of(long warehouseId, Supplier<ComputeResource> lazy) {
        return new LazyComputeResource(warehouseId, lazy);
    }

    public ComputeResource get() {
        if (LOG.isDebugEnabled()) {
            String queryId = ConnectContext.get() != null ? ConnectContext.get().getQueryId().toString() : "N/A";
            LOG.debug("Materializing ComputeResource in LazyComputeResource, queryId: {}", queryId);
        }

        ComputeResource result = lazy.get();
        initialized.set(true);
        return result;
    }

    public boolean isInitialized() {
        return initialized.get();
    }

    @Override
    public long getWarehouseId() {
        return warehouseId;
    }

    @Override
    public long getWorkerGroupId() {
        return get().getWorkerGroupId();
    }

    @Override
    public String toString() {
        return "{warehouseId=" + warehouseId +
                ", computeResource=" + (isInitialized() ? get().toString() : "not initialized") +
                "}";
    }
}
