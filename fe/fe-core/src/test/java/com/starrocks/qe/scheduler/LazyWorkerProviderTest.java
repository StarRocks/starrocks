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

package com.starrocks.qe.scheduler;

import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class LazyWorkerProviderTest {
    
    /**
     * Simple stub implementation of WorkerProvider for testing purposes.
     */
    private static class StubWorkerProvider implements WorkerProvider {
        private final String name;
        
        public StubWorkerProvider(String name) {
            this.name = name;
        }
        
        @Override
        public String toString() {
            return name;
        }
        
        @Override
        public long selectNextWorker() throws NonRecoverableException {
            return 0;
        }
        
        @Override
        public void selectWorker(long workerId) throws NonRecoverableException {
        }
        
        @Override
        public List<Long> selectAllComputeNodes() {
            return Collections.emptyList();
        }
        
        @Override
        public Collection<ComputeNode> getAllWorkers() {
            return Collections.emptyList();
        }
        
        @Override
        public ComputeNode getWorkerById(long workerId) {
            return null;
        }
        
        @Override
        public boolean isDataNodeAvailable(long dataNodeId) {
            return false;
        }
        
        @Override
        public void reportDataNodeNotFoundException() throws NonRecoverableException {
        }

        @Override
        public void reportWorkerNotFoundException() throws NonRecoverableException {

        }


        @Override
        public boolean isWorkerSelected(long workerId) {
            return false;
        }
        
        @Override
        public List<Long> getSelectedWorkerIds() {
            return Collections.emptyList();
        }

        @Override
        public List<Long> getAllAvailableNodes() {
            return List.of();
        }

        @Override
        public void selectWorkerUnchecked(long workerId) {

        }


        @Override
        public boolean allowUsingBackupNode() {
            return false;
        }

        @Override
        public long selectBackupWorker(long workerId) {
            return 0;
        }

        @Override
        public ComputeResource getComputeResource() {
            return null;
        }
    }
    
    @Test
    public void capturesWorkerProviderLazily() {
        WorkerProvider mockWorkerProvider = new StubWorkerProvider("MockWorkerProvider");

        LazyWorkerProvider lazyWorkerProvider = LazyWorkerProvider.of(() -> mockWorkerProvider);
        Assertions.assertNotNull(lazyWorkerProvider);
        Assertions.assertEquals("MockWorkerProvider", lazyWorkerProvider.get().toString());
    }

    @Test
    public void memoizesSupplierInvocationOnce() {
        java.util.concurrent.atomic.AtomicInteger invocationCount = new java.util.concurrent.atomic.AtomicInteger();
        WorkerProvider mockWorkerProvider = new StubWorkerProvider("MemoizedProvider");

        LazyWorkerProvider lazyWorkerProvider = LazyWorkerProvider.of(() -> {
            invocationCount.incrementAndGet();
            return mockWorkerProvider;
        });

        Assertions.assertEquals(0, invocationCount.get());
        WorkerProvider first = lazyWorkerProvider.get();
        WorkerProvider second = lazyWorkerProvider.get();
        Assertions.assertSame(first, second);
        Assertions.assertEquals(1, invocationCount.get());
    }

    @Test
    public void returnsNullWhenSupplierProvidesNull() {
        LazyWorkerProvider lazyWorkerProvider = LazyWorkerProvider.of(() -> null);
        Assertions.assertNull(lazyWorkerProvider.get());
        // Subsequent calls should still return null (memoized)
        Assertions.assertNull(lazyWorkerProvider.get());
    }

    @Test
    public void materializesWithConnectContextPresent() {
        WorkerProvider mockWorkerProvider = new StubWorkerProvider("ProviderWithContext");

        // Test that LazyWorkerProvider can materialize when ConnectContext.get() returns null
        // This simulates the case where no query context is present
        LazyWorkerProvider lazyWorkerProvider = LazyWorkerProvider.of(() -> mockWorkerProvider);
        WorkerProvider result = lazyWorkerProvider.get();
        
        // Verify the provider is returned correctly
        Assertions.assertNotNull(result);
        Assertions.assertEquals("ProviderWithContext", result.toString());
        // Verify memoization - should return the same instance
        Assertions.assertSame(result, lazyWorkerProvider.get());
    }

    @Test
    public void handlesConnectContextNullGracefully() {
        WorkerProvider mockWorkerProvider = new StubWorkerProvider("ProviderWithNullContext");

        // Test that LazyWorkerProvider works even when ConnectContext.get() returns null
        // This is the normal case when no query context is active
        LazyWorkerProvider lazyWorkerProvider = LazyWorkerProvider.of(() -> mockWorkerProvider);
        
        // The get() method should work fine even with null ConnectContext
        WorkerProvider result = lazyWorkerProvider.get();
        Assertions.assertNotNull(result);
        Assertions.assertEquals("ProviderWithNullContext", result.toString());
    }
}
