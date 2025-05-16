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

import com.starrocks.thrift.TGetWarehouseMetricsResponeItem;
import org.junit.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class WarehouseMetricsTest {
    @Test
    public void testCreateEmptyWarehouseMetrics() {
        WarehouseMetrics empty = WarehouseMetrics.empty();
        TGetWarehouseMetricsResponeItem thrift = empty.toThrift();
        assertThat(thrift.getWarehouse_id()).isEqualTo("0");
        assertThat(thrift.getWarehouse_name()).isEqualTo("");
        assertThat(thrift.getQueue_pending_length()).isEqualTo("0");
        assertThat(thrift.getQueue_running_length()).isEqualTo("0");
        assertThat(thrift.getMax_pending_length()).isEqualTo("0");
        assertThat(thrift.getMax_pending_time_second()).isEqualTo("0");
        assertThat(thrift.getEarliest_query_wait_time()).isEqualTo("0.0");
        assertThat(thrift.getMax_required_slots()).isEqualTo("0");
        assertThat(thrift.getSum_required_slots()).isEqualTo("0");
        assertThat(thrift.getRemain_slots()).isEqualTo("0");
        assertThat(thrift.getMax_slots()).isEqualTo("0");
    }

    @Test
    public void testCreateWarehouseMetrics() {
        WarehouseMetrics warehouseMetrics = new WarehouseMetrics(1, "test", 1, 1, 1, 1, 1, 1, 1, 1, 1,
                Optional.empty());
        TGetWarehouseMetricsResponeItem thrift = warehouseMetrics.toThrift();
        assertThat(thrift.getWarehouse_id()).isEqualTo("1");
        assertThat(thrift.getWarehouse_name()).isEqualTo("test");
        assertThat(thrift.getQueue_pending_length()).isEqualTo("1");
        assertThat(thrift.getQueue_running_length()).isEqualTo("1");
        assertThat(thrift.getMax_pending_length()).isEqualTo("1");
        assertThat(thrift.getMax_pending_time_second()).isEqualTo("1");
        assertThat(thrift.getEarliest_query_wait_time()).isEqualTo("1.0");
        assertThat(thrift.getMax_required_slots()).isEqualTo("1");
        assertThat(thrift.getSum_required_slots()).isEqualTo("1");
        assertThat(thrift.getRemain_slots()).isEqualTo("1");
        assertThat(thrift.getMax_slots()).isEqualTo("1");
    }
}
