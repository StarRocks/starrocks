// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe.scheduler.warehouse;

import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.thrift.TGetWarehouseQueriesResponseItem;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class WarehouseQueryMetricsTest {
    @Test
    public void testCreateEmptyWarehouseQueryMetrics() {
        WarehouseQueryMetrics empty = WarehouseQueryMetrics.empty();
        TGetWarehouseQueriesResponseItem thrift = empty.toThrift();
        assertThat(thrift.getWarehouse_id()).isEqualTo("0");
        assertThat(thrift.getWarehouse_name()).isEqualTo("");
        assertThat(thrift.getQuery_id()).isEqualTo("00000000-0000-0000-0000-000000000000");
        assertThat(thrift.getState()).isEqualTo("CREATED");
        assertThat(thrift.getEst_costs_slots()).isEqualTo("0");
        assertThat(thrift.getAllocate_slots()).isEqualTo("0");
        assertThat(thrift.getQueued_wait_seconds()).isEqualTo("0.0");
    }

    @Test
    public void testCreateWarehouseQueryMetrics() {
        WarehouseQueryMetrics metrics = new WarehouseQueryMetrics(1, "test",
                null, LogicalSlot.State.ALLOCATED, 1, 1, 1);
        TGetWarehouseQueriesResponseItem thrift = metrics.toThrift();
        assertThat(thrift.getWarehouse_id()).isEqualTo("1");
        assertThat(thrift.getWarehouse_name()).isEqualTo("test");
        assertThat(thrift.getQuery_id()).isEqualTo("");
        assertThat(thrift.getState()).isEqualTo("ALLOCATED");
        assertThat(thrift.getEst_costs_slots()).isEqualTo("1");
        assertThat(thrift.getAllocate_slots()).isEqualTo("1");
        assertThat(thrift.getQueued_wait_seconds()).isEqualTo("1.0");
    }
}
