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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LazyComputeResourceTest {
    
    /**
     * Simple stub implementation of ComputeResource for testing purposes.
     */
    private static class StubComputeResource implements ComputeResource {
        private final long warehouseId;
        private final long workerGroupId;
        private final String name;
        
        public StubComputeResource(long warehouseId, long workerGroupId, String name) {
            this.warehouseId = warehouseId;
            this.workerGroupId = workerGroupId;
            this.name = name;
        }
        
        @Override
        public long getWarehouseId() {
            return warehouseId;
        }
        
        @Override
        public long getWorkerGroupId() {
            return workerGroupId;
        }
        
        @Override
        public String toString() {
            return name;
        }
    }
    
    @Test
    public void capturesComputeResourceLazily() {
        ComputeResource mockComputeResource = new StubComputeResource(123L, 456L, "MockComputeResource");

        LazyComputeResource lazyComputeResource = LazyComputeResource.of(123L, () -> mockComputeResource);
        Assertions.assertNotNull(lazyComputeResource);
        Assertions.assertEquals(123L, lazyComputeResource.getWarehouseId());
        Assertions.assertEquals(456L, lazyComputeResource.getWorkerGroupId());
    }

    @Test
    public void memoizesSupplierInvocationOnce() {
        java.util.concurrent.atomic.AtomicInteger invocationCount = new java.util.concurrent.atomic.AtomicInteger();
        ComputeResource mockComputeResource = new StubComputeResource(789L, 101112L, "MemoizedComputeResource");

        LazyComputeResource lazyComputeResource = LazyComputeResource.of(789L, () -> {
            invocationCount.incrementAndGet();
            return mockComputeResource;
        });

        Assertions.assertEquals(0, invocationCount.get());
        ComputeResource first = lazyComputeResource.get();
        ComputeResource second = lazyComputeResource.get();
        Assertions.assertSame(first, second);
        Assertions.assertEquals(1, invocationCount.get());
    }

    @Test
    public void returnsNullWhenSupplierProvidesNull() {
        LazyComputeResource lazyComputeResource = LazyComputeResource.of(999L, () -> null);
        Assertions.assertNull(lazyComputeResource.get());
        // Subsequent calls should still return null (memoized)
        Assertions.assertNull(lazyComputeResource.get());
    }

    @Test
    public void materializesWithConnectContextPresent() {
        ComputeResource mockComputeResource = new StubComputeResource(111L, 222L, "ProviderWithContext");

        // Test that LazyComputeResource can materialize when ConnectContext.get() returns null
        // This simulates the case where no query context is present
        LazyComputeResource lazyComputeResource = LazyComputeResource.of(111L, () -> mockComputeResource);
        ComputeResource result = lazyComputeResource.get();
        
        // Verify the provider is returned correctly
        Assertions.assertNotNull(result);
        Assertions.assertEquals("ProviderWithContext", result.toString());
        // Verify memoization - should return the same instance
        Assertions.assertSame(result, lazyComputeResource.get());
    }

    @Test
    public void handlesConnectContextNullGracefully() {
        ComputeResource mockComputeResource = new StubComputeResource(333L, 444L, "ProviderWithNullContext");

        // Test that LazyComputeResource works even when ConnectContext.get() returns null
        // This is the normal case when no query context is active
        LazyComputeResource lazyComputeResource = LazyComputeResource.of(333L, () -> mockComputeResource);
        
        // The get() method should work fine even with null ConnectContext
        ComputeResource result = lazyComputeResource.get();
        Assertions.assertNotNull(result);
        Assertions.assertEquals("ProviderWithNullContext", result.toString());
    }

    @Test
    public void delegatesWarehouseIdCorrectly() {
        long expectedWarehouseId = 555L;
        ComputeResource mockComputeResource = new StubComputeResource(expectedWarehouseId, 666L, "WarehouseTest");

        LazyComputeResource lazyComputeResource = LazyComputeResource.of(expectedWarehouseId, () -> mockComputeResource);
        
        // Verify that the warehouse ID is stored and returned correctly
        Assertions.assertEquals(expectedWarehouseId, lazyComputeResource.getWarehouseId());
    }

    @Test
    public void delegatesWorkerGroupIdCorrectly() {
        long expectedWorkerGroupId = 777L;
        ComputeResource mockComputeResource = new StubComputeResource(888L, expectedWorkerGroupId, "WorkerGroupTest");

        LazyComputeResource lazyComputeResource = LazyComputeResource.of(888L, () -> mockComputeResource);
        
        // Verify that the worker group ID is delegated correctly
        Assertions.assertEquals(expectedWorkerGroupId, lazyComputeResource.getWorkerGroupId());
    }

    @Test
    public void handlesMultipleInstancesIndependently() {
        ComputeResource resource1 = new StubComputeResource(100L, 200L, "Resource1");
        ComputeResource resource2 = new StubComputeResource(300L, 400L, "Resource2");

        LazyComputeResource lazy1 = LazyComputeResource.of(100L, () -> resource1);
        LazyComputeResource lazy2 = LazyComputeResource.of(300L, () -> resource2);

        // Verify that each instance works independently
        Assertions.assertEquals(100L, lazy1.getWarehouseId());
        Assertions.assertEquals(200L, lazy1.getWorkerGroupId());
        Assertions.assertEquals("Resource1", lazy1.get().toString());

        Assertions.assertEquals(300L, lazy2.getWarehouseId());
        Assertions.assertEquals(400L, lazy2.getWorkerGroupId());
        Assertions.assertEquals("Resource2", lazy2.get().toString());
    }
}
