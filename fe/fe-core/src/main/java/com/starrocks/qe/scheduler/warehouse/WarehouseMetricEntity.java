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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.metric.GaugeMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricLabel;
import com.starrocks.qe.scheduler.slot.BaseSlotTracker;
import com.starrocks.qe.scheduler.slot.QueryQueueOptions;

import java.util.List;

public class WarehouseMetricEntity {
    private final long warehouseId;
    private final String warehouseName;
    private final BaseSlotTracker tracker;

    public final GaugeMetric<Long> queuePendingLength;
    public final GaugeMetric<Long> queueRunningLength;
    public final GaugeMetric<Long> maxQueueQueueLength;
    public final GaugeMetric<Double> earliestQueryWaitTime;
    public final GaugeMetric<Long> maxQueuePendingTimeSecond;
    public final GaugeMetric<Integer> maxRequiredSlots;
    public final GaugeMetric<Integer> sumRequiredSlots;
    public final GaugeMetric<Long> remainSlots;
    public final GaugeMetric<Long> maxSlots;

    private final List<Metric> metrics = Lists.newArrayList();

    public WarehouseMetricEntity(BaseSlotTracker tracker) {
        Preconditions.checkArgument(tracker != null);

        this.warehouseId = tracker.getWarehouseId();
        this.warehouseName = tracker.getWarehouseName();
        this.tracker = tracker;

        this.queuePendingLength = new GaugeMetric<Long>("warehouse_query_queue", Metric.MetricUnit.NOUNIT,
                "current warehouse query pending length") {
            @Override
            public Long getValue() {
                return tracker.getQueuePendingLength();
            }
        };
        queuePendingLength.addLabel(new MetricLabel("field", "query_pending_length"));
        metrics.add(queuePendingLength);

        this.queueRunningLength = new GaugeMetric<Long>("warehouse_query_queue", Metric.MetricUnit.NOUNIT,
                "current warehouse query running length") {
            @Override
            public Long getValue() {
                return (long) tracker.getNumAllocatedSlots();
            }
        };
        queueRunningLength.addLabel(new MetricLabel("field", "query_running_length"));
        metrics.add(queueRunningLength);

        this.maxQueueQueueLength = new GaugeMetric<Long>("warehouse_query_queue", Metric.MetricUnit.NOUNIT,
                "current warehouse max query queue length") {
            @Override
            public Long getValue() {
                return tracker.getMaxQueueQueueLength();
            }
        };
        maxQueueQueueLength.addLabel(new MetricLabel("field", "max_query_queue_length"));
        metrics.add(maxQueueQueueLength);

        this.earliestQueryWaitTime = new GaugeMetric<Double>("warehouse_query_queue", Metric.MetricUnit.NOUNIT,
                "current warehouse earliest query wait time(if not set, return 0.0)") {
            @Override
            public Double getValue() {
                return tracker.getEarliestQueryWaitTimeSecond();
            }
        };
        earliestQueryWaitTime.addLabel(new MetricLabel("field", "earliest_query_wait_time"));
        metrics.add(earliestQueryWaitTime);

        this.maxQueuePendingTimeSecond = new GaugeMetric<Long>("warehouse_query_queue", Metric.MetricUnit.NOUNIT,
                "current warehouse queue max pending time second") {
            @Override
            public Long getValue() {
                return tracker.getMaxQueuePendingTimeSecond();
            }
        };
        maxQueuePendingTimeSecond.addLabel(new MetricLabel("field", "max_query_pending_time_second"));
        metrics.add(maxQueuePendingTimeSecond);

        this.maxRequiredSlots = new GaugeMetric<Integer>("warehouse_query_queue", Metric.MetricUnit.NOUNIT,
                "current warehouse the max of all required(but not allocated) slots") {
            @Override
            public Integer getValue() {
                return tracker.getMaxRequiredSlots().map(s -> QueryQueueOptions.correctSlotNum(s)).orElse(0);
            }
        };
        maxRequiredSlots.addLabel(new MetricLabel("field", "max_required_slots"));
        metrics.add(maxRequiredSlots);

        this.sumRequiredSlots = new GaugeMetric<Integer>("warehouse_query_queue", Metric.MetricUnit.NOUNIT,
                "current warehouse the sum of all the required(but not allocated) slots") {
            @Override
            public Integer getValue() {
                return tracker.getSumRequiredSlots().map(s -> QueryQueueOptions.correctSlotNum(s)).orElse(0);
            }
        };
        sumRequiredSlots.addLabel(new MetricLabel("field", "sum_required_slots"));
        metrics.add(sumRequiredSlots);

        this.remainSlots = new GaugeMetric<Long>("warehouse_query_queue", Metric.MetricUnit.NOUNIT,
                "current warehouse remain slots") {
            @Override
            public Long getValue() {
                return (long) QueryQueueOptions.correctSlotNum(tracker.getRemainSlots().orElse(0));
            }
        };
        remainSlots.addLabel(new MetricLabel("field", "remain_slots"));
        metrics.add(remainSlots);

        this.maxSlots = new GaugeMetric<Long>("warehouse_query_queue", Metric.MetricUnit.NOUNIT,
                "current warehouse query pending length") {
            @Override
            public Long getValue() {
                return (long) tracker.getMaxSlots().map(s -> QueryQueueOptions.correctSlotNum(s)).orElse(0);
            }
        };
        maxSlots.addLabel(new MetricLabel("field", "max_slots"));
        metrics.add(maxSlots);
    }

    public List<Metric> getMetrics() {
        return this.metrics;
    }

    public long getWarehouseId() {
        return warehouseId;
    }

    public String getWarehouseName() {
        return warehouseName;
    }
}
