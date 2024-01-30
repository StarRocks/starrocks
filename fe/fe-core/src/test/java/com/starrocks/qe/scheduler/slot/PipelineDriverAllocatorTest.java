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

package com.starrocks.qe.scheduler.slot;

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.system.BackendCoreStat;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PipelineDriverAllocatorTest {
    private int prevDriverLowWater;
    private int prevDriverHighWater;

    private final int numCpuCores = 64;

    @Before
    public void before() {
        prevDriverLowWater = GlobalVariable.getQueryQueueDriverLowWater();
        prevDriverHighWater = GlobalVariable.getQueryQueueDriverHighWater();

        mockCPUCores(numCpuCores);
    }

    @After
    public void after() {
        GlobalVariable.setQueryQueueDriverLowWater(prevDriverLowWater);
        GlobalVariable.setQueryQueueDriverHighWater(prevDriverHighWater);
    }

    @Test
    public void testDisableDriverHighWater() {
        PipelineDriverAllocator allocator = new PipelineDriverAllocator();

        GlobalVariable.setQueryQueueDriverHighWater(-1);

        LogicalSlot slot = genSlot(7, 10);

        allocator.allocate(slot);
        assertThat(slot.getNumDrivers()).isEqualTo(7 * 10);
        assertThat(allocator.getNumAllocatedDrivers()).isZero();

        allocator.release(slot);
        assertThat(slot.getNumDrivers()).isEqualTo(7 * 10);
        assertThat(allocator.getNumAllocatedDrivers()).isZero();
    }

    @Test
    public void testDisableDriverLowWater() {
        PipelineDriverAllocator allocator = new PipelineDriverAllocator();

        GlobalVariable.setQueryQueueDriverHighWater(0);
        GlobalVariable.setQueryQueueDriverLowWater(-1);

        LogicalSlot slot1 = genSlot(2, 0);
        LogicalSlot slot2 = genSlot(GlobalVariable.getQueryQueueDriverHighWater() / 3, 0);

        allocator.allocate(slot1);
        allocator.allocate(slot2);
        assertThat(slot2.getPipelineDop()).isEqualTo(2);
        assertThat(allocator.getNumAllocatedDrivers()).isEqualTo(slot2.getNumDrivers() + slot1.getNumDrivers());

        allocator.release(slot1);
        allocator.release(slot2);
        assertThat(slot2.getPipelineDop()).isEqualTo(2);
        assertThat(allocator.getNumAllocatedDrivers()).isZero();
    }

    @Test
    public void testNonAdaptiveDop() {
        PipelineDriverAllocator allocator = new PipelineDriverAllocator();

        GlobalVariable.setQueryQueueDriverHighWater(-0);

        LogicalSlot slot = genSlot(7, 10);

        allocator.allocate(slot);
        allocator.allocate(slot);
        assertThat(slot.getNumDrivers()).isEqualTo(7 * 10);
        assertThat(allocator.getNumAllocatedDrivers()).isEqualTo(7 * 10);

        allocator.release(slot);
        assertThat(slot.getNumDrivers()).isEqualTo(7 * 10);
        assertThat(allocator.getNumAllocatedDrivers()).isZero();
    }

    @Test
    public void testSmallFragments() {
        PipelineDriverAllocator allocator = new PipelineDriverAllocator();

        GlobalVariable.setQueryQueueDriverHighWater(0);
        GlobalVariable.setQueryQueueDriverLowWater(0);

        LogicalSlot slot1 = genSlot(2, 0);
        LogicalSlot slot2 = genSlot(3, 0);

        allocator.allocate(slot1);
        allocator.allocate(slot1);
        assertThat(slot1.getNumDrivers()).isEqualTo(2 * BackendCoreStat.getDefaultDOP());
        assertThat(allocator.getNumAllocatedDrivers()).isEqualTo(slot1.getNumDrivers());

        allocator.allocate(slot2);
        allocator.allocate(slot2);
        assertThat(slot2.getNumDrivers()).isEqualTo(3 * BackendCoreStat.getDefaultDOP());
        assertThat(allocator.getNumAllocatedDrivers()).isEqualTo(slot1.getNumDrivers() + slot2.getNumDrivers());

        allocator.release(slot1);
        allocator.release(slot1);
        assertThat(slot1.getNumDrivers()).isEqualTo(2 * BackendCoreStat.getDefaultDOP());
        assertThat(allocator.getNumAllocatedDrivers()).isEqualTo(slot2.getNumDrivers());

        allocator.release(slot2);
        allocator.release(slot2);
        assertThat(slot2.getNumDrivers()).isEqualTo(3 * BackendCoreStat.getDefaultDOP());
        assertThat(allocator.getNumAllocatedDrivers()).isZero();
    }

    @Test
    public void testLargeFragments() {
        PipelineDriverAllocator allocator = new PipelineDriverAllocator();

        GlobalVariable.setQueryQueueDriverHighWater(0);
        GlobalVariable.setQueryQueueDriverLowWater(0);

        LogicalSlot slot = genSlot(GlobalVariable.getQueryQueueDriverHighWater() * 2, 0);

        allocator.allocate(slot);
        allocator.allocate(slot);
        assertThat(slot.getPipelineDop()).isEqualTo(1);
        assertThat(allocator.getNumAllocatedDrivers()).isEqualTo(slot.getNumDrivers());

        allocator.release(slot);
        allocator.release(slot);
        assertThat(slot.getPipelineDop()).isEqualTo(1);
        assertThat(allocator.getNumAllocatedDrivers()).isZero();
    }

    @Test
    public void testExceedSoftLimit() {
        PipelineDriverAllocator allocator = new PipelineDriverAllocator();

        GlobalVariable.setQueryQueueDriverHighWater(0);
        GlobalVariable.setQueryQueueDriverLowWater(0);

        LogicalSlot slot1 = genSlot(2, 0);
        LogicalSlot slot2 = genSlot(numCpuCores, 0);

        allocator.allocate(slot1);
        allocator.allocate(slot2);
        assertThat(slot2.getPipelineDop()).isGreaterThan(1);

        allocator.release(slot2);
        allocator.release(slot1);
        assertThat(allocator.getNumAllocatedDrivers()).isZero();
    }

    @Test
    public void testConcurrency() throws InterruptedException {
        PipelineDriverAllocator allocator = new PipelineDriverAllocator();

        mockCPUCores(PipelineDriverAllocator.NUM_BATCH_SLOTS * 1000);
        GlobalVariable.setQueryQueueDriverHighWater(0);
        GlobalVariable.setQueryQueueDriverLowWater(0);

        final int numSlots = PipelineDriverAllocator.NUM_BATCH_SLOTS * 2;

        List<Thread> threads = new ArrayList<>(numSlots);
        List<LogicalSlot> slots = new ArrayList<>(numSlots);
        for (int i = 0; i < numSlots; i++) {
            LogicalSlot slot = genSlot(2, 0);
            Thread thread = new Thread(() -> allocator.allocate(slot));
            threads.add(thread);
            slots.add(slot);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        for (LogicalSlot slot : slots) {
            assertThat(slot.getPipelineDop()).isGreaterThanOrEqualTo(1);
        }
    }

    /**
     * Mock {@link BackendCoreStat#getAvgNumOfHardwareCoresOfBe()}.
     */
    private void mockCPUCores(int numCpuCores) {
        new MockUp<BackendCoreStat>() {
            @Mock
            public int getAvgNumOfHardwareCoresOfBe() {
                return numCpuCores;
            }
        };
    }

    private LogicalSlot genSlot(int numFragments, int dop) {
        return new LogicalSlot(UUIDUtil.genTUniqueId(), "fe-name", 0, 1, 0, 0, 0, numFragments, dop);
    }
}
