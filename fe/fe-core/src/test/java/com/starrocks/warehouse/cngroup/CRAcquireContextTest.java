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

package com.starrocks.warehouse.cngroup;

import com.starrocks.common.ErrorReportException;
import com.starrocks.warehouse.WarehouseTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class CRAcquireContextTest extends WarehouseTestBase {
    @Test
    public void testCNAcquireContext1() {
        CRAcquireContext context = CRAcquireContext.of(1L, CRAcquireStrategy.STANDARD);
        assertThat(context != null).isTrue();
        assertThat(context.getWarehouseId() == 1L).isTrue();
        assertThat(context.getStrategy() == CRAcquireStrategy.STANDARD).isTrue();
        assertThat(context.getPrevComputeResource() == null).isTrue();
    }

    @Test
    public void testCNAcquireContext2() {
        ComputeResource prevComputeResource = new ComputeResource() {
            @Override
            public long getWarehouseId() {
                return 1L;
            }

            @Override
            public long getWorkerGroupId() {
                return 2L;
            }
        };

        CRAcquireContext context = CRAcquireContext.of(1L, CRAcquireStrategy.STANDARD, prevComputeResource);
        assertThat(context != null).isTrue();
        assertThat(context.getWarehouseId() == 1L).isTrue();
        assertThat(context.getStrategy() == CRAcquireStrategy.STANDARD).isTrue();
        assertThat(context.getPrevComputeResource() == prevComputeResource).isTrue();
    }

    @Test
    public void testCNAcquireContext3() {
        CRAcquireContext context = CRAcquireContext.of(1L);
        assertThat(context != null).isTrue();
        assertThat(context.getWarehouseId() == 1L).isTrue();
        assertThat(context.getStrategy() == CRAcquireStrategy.STANDARD).isTrue();
        assertThat(context.getPrevComputeResource() == null).isTrue();
    }

    @Test
    public void testCNAcquireContext4() {
        ComputeResource prevComputeResource = new ComputeResource() {
            @Override
            public long getWarehouseId() {
                return 1L;
            }

            @Override
            public long getWorkerGroupId() {
                return 2L;
            }
        };

        CRAcquireContext context = CRAcquireContext.of(1L, prevComputeResource);
        assertThat(context != null).isTrue();
        assertThat(context.getWarehouseId() == 1L).isTrue();
        assertThat(context.getStrategy() == CRAcquireStrategy.STANDARD).isTrue();
        assertThat(context.getPrevComputeResource() == prevComputeResource).isTrue();
    }

    @Test
    public void testCNAcquireContext5() {
        try {
            CRAcquireContext.of("bad_warehouse");
            Assertions.fail();
        } catch (ErrorReportException e) {
            e.printStackTrace();
            assertThat(e.getMessage()).contains("Warehouse name: bad_warehouse not exist");
        }
    }
}
