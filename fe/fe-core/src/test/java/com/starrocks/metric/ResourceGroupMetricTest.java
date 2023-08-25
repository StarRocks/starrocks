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

package com.starrocks.metric;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ResourceGroupMetricTest {

    @BeforeClass
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @Test
    public void testResourceGroupQueryQueueMetrics() {
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        ctx.setSessionVariable(sessionVariable);

        ResourceGroup wg1 = new ResourceGroup();
        wg1.setName("wg1");
        ResourceGroup wg2 = new ResourceGroup();
        wg2.setName("wg2");

        // Increase.
        ctx.setResourceGroup(wg1.toThrift());
        ResourceGroupMetricMgr.increaseQueuedQuery(ctx, 10L);
        ResourceGroupMetricMgr.increaseTimeoutQueuedQuery(ctx, 1L);

        ctx.setResourceGroup(wg2.toThrift());
        ResourceGroupMetricMgr.increaseQueuedQuery(ctx, 20L);
        ResourceGroupMetricMgr.increaseTimeoutQueuedQuery(ctx, 2L);

        ctx.setResourceGroup(null);
        ResourceGroupMetricMgr.increaseQueuedQuery(ctx, 30L);
        ResourceGroupMetricMgr.increaseTimeoutQueuedQuery(ctx, 3L);

        ctx.getSessionVariable().setEnablePipelineEngine(false);
        ResourceGroupMetricMgr.increaseQueuedQuery(ctx, 40L);
        ResourceGroupMetricMgr.increaseTimeoutQueuedQuery(ctx, 4L);
        ctx.getSessionVariable().setEnablePipelineEngine(true);

        List<Metric> metricTotal = MetricRepo.getMetricsByName("resource_group_query_queue_total");
        List<Metric> metricPending = MetricRepo.getMetricsByName("resource_group_query_queue_pending");
        List<Metric> metricTimeout = MetricRepo.getMetricsByName("resource_group_query_queue_timeout");

        Assert.assertEquals(4, metricTotal.size());
        Assert.assertEquals(4, metricPending.size());
        Assert.assertEquals(4, metricTimeout.size());

        // Check.
        Map<String, Long> groupToTotal = metricTotal.stream().map(m -> (LongCounterMetric) m).collect(Collectors.toMap(
                m -> m.getLabels().get(0).getValue(),
                LongCounterMetric::getValue
        ));
        Map<String, Long> expectedGroupToTotal = ImmutableMap.of(
                wg1.getName(), 10L,
                wg2.getName(), 20L,
                ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME, 30L,
                ResourceGroup.DISABLE_RESOURCE_GROUP_NAME, 40L
        );
        assertThat(groupToTotal).containsExactlyInAnyOrderEntriesOf(expectedGroupToTotal);

        Map<String, Long> groupToPending = metricPending.stream().map(m -> (LongCounterMetric) m).collect(Collectors.toMap(
                m -> m.getLabels().get(0).getValue(),
                LongCounterMetric::getValue
        ));
        assertThat(groupToPending).containsExactlyInAnyOrderEntriesOf(expectedGroupToTotal);

        Map<String, Long> groupToTimeout = metricTimeout.stream().map(m -> (LongCounterMetric) m).collect(Collectors.toMap(
                m -> m.getLabels().get(0).getValue(),
                LongCounterMetric::getValue
        ));
        Map<String, Long> expectedGroupToTimeout = ImmutableMap.of(
                wg1.getName(), 1L,
                wg2.getName(), 2L,
                ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME, 3L,
                ResourceGroup.DISABLE_RESOURCE_GROUP_NAME, 4L
        );
        assertThat(groupToTimeout).containsExactlyInAnyOrderEntriesOf(expectedGroupToTimeout);

        // Decrease.
        ctx.setResourceGroup(wg1.toThrift());
        ResourceGroupMetricMgr.increaseQueuedQuery(ctx, -1L);

        ctx.setResourceGroup(wg2.toThrift());
        ResourceGroupMetricMgr.increaseQueuedQuery(ctx, -2L);

        ctx.setResourceGroup(null);
        ResourceGroupMetricMgr.increaseQueuedQuery(ctx, -3L);

        ctx.getSessionVariable().setEnablePipelineEngine(false);
        ResourceGroupMetricMgr.increaseQueuedQuery(ctx, -4L);
        ctx.getSessionVariable().setEnablePipelineEngine(true);

        // Check.
        groupToTotal = metricTotal.stream().map(m -> (LongCounterMetric) m).collect(Collectors.toMap(
                m -> m.getLabels().get(0).getValue(),
                LongCounterMetric::getValue
        ));
        assertThat(groupToTotal).containsExactlyInAnyOrderEntriesOf(expectedGroupToTotal);

        groupToPending = metricPending.stream().map(m -> (LongCounterMetric) m).collect(Collectors.toMap(
                m -> m.getLabels().get(0).getValue(),
                LongCounterMetric::getValue
        ));
        Map<String, Long> expectedGroupToPending = ImmutableMap.of(
                wg1.getName(), 10L - 1L,
                wg2.getName(), 20L - 2L,
                ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME, 30L - 3L,
                ResourceGroup.DISABLE_RESOURCE_GROUP_NAME, 40L - 4L
        );
        assertThat(groupToPending).containsExactlyInAnyOrderEntriesOf(expectedGroupToPending);

        groupToTimeout = metricTimeout.stream().map(m -> (LongCounterMetric) m).collect(Collectors.toMap(
                m -> m.getLabels().get(0).getValue(),
                LongCounterMetric::getValue
        ));
        assertThat(groupToTimeout).containsExactlyInAnyOrderEntriesOf(expectedGroupToTimeout);
    }

    @Test
    public void testResourceGroupMetrics() {
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        ctx.setSessionVariable(sessionVariable);

        ResourceGroup wg1 = new ResourceGroup();
        wg1.setName("wg1");
        ResourceGroup wg2 = new ResourceGroup();
        wg2.setName("wg2");

        ctx.setResourceGroup(wg1.toThrift());
        ctx.getAuditEventBuilder().setResourceGroup(wg1.getName());
        ResourceGroupMetricMgr.increaseQuery(ctx, 1L);
        ResourceGroupMetricMgr.increaseQueryErr(ctx, 1L);
        ResourceGroupMetricMgr.updateQueryLatency(ctx, 10L);

        ctx.setResourceGroup(wg2.toThrift());
        ctx.getAuditEventBuilder().setResourceGroup(wg2.getName());
        ResourceGroupMetricMgr.increaseQuery(ctx, 1L);
        ResourceGroupMetricMgr.increaseQueryErr(ctx, 1L);
        ResourceGroupMetricMgr.updateQueryLatency(ctx, 10L);

        ctx.setResourceGroup(null);
        ctx.getAuditEventBuilder().setResourceGroup(ResourceGroup.DISABLE_RESOURCE_GROUP_NAME);
        ResourceGroupMetricMgr.increaseQuery(ctx, 2L);
        ResourceGroupMetricMgr.increaseQueryErr(ctx, 2L);
        ResourceGroupMetricMgr.updateQueryLatency(ctx, 20L);

        ctx.getSessionVariable().setEnablePipelineEngine(false);
        ctx.getAuditEventBuilder().setResourceGroup("");
        ResourceGroupMetricMgr.increaseQuery(ctx, 3L);
        ResourceGroupMetricMgr.increaseQueryErr(ctx, 3L);
        ResourceGroupMetricMgr.updateQueryLatency(ctx, 30L);

        List<Metric> metricsResourceGroup = MetricRepo.getMetricsByName("query_resource_group");
        List<Metric> metricsResourceGroupErr = MetricRepo.getMetricsByName("query_resource_group_err");
        List<Metric> metricsResourceGroupLatency = MetricRepo.getMetricsByName("query_resource_group_latency");

        Assert.assertEquals(4, metricsResourceGroup.size());
        Assert.assertEquals(4, metricsResourceGroupErr.size());
        Assert.assertEquals(4 * 6, metricsResourceGroupLatency.size());

        for (Metric resourceGroupMetric : metricsResourceGroup) {
            LongCounterMetric metric = (LongCounterMetric) resourceGroupMetric;
            if (wg1.getName().equals(metric.getLabels().get(0).getValue())) {
                Assert.assertEquals(Long.valueOf(1L), metric.getValue());
            } else if (wg2.getName().equals(metric.getLabels().get(0).getValue())) {
                Assert.assertEquals(Long.valueOf(1L), metric.getValue());
            } else if (ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME.equals(metric.getLabels().get(0).getValue())) {
                Assert.assertEquals(Long.valueOf(2L), metric.getValue());
            } else if (ResourceGroup.DISABLE_RESOURCE_GROUP_NAME.equals(metric.getLabels().get(0).getValue())) {
                Assert.assertEquals(Long.valueOf(3L), metric.getValue());
            } else {
                Assert.fail();
            }
        }

        for (Metric resourceGroupMetric : metricsResourceGroupErr) {
            LongCounterMetric metric = (LongCounterMetric) resourceGroupMetric;
            if (wg1.getName().equals(metric.getLabels().get(0).getValue())) {
                Assert.assertEquals(Long.valueOf(1L), metric.getValue());
            } else if (wg2.getName().equals(metric.getLabels().get(0).getValue())) {
                Assert.assertEquals(Long.valueOf(1L), metric.getValue());
            } else if (ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME.equals(metric.getLabels().get(0).getValue())) {
                Assert.assertEquals(Long.valueOf(2L), metric.getValue());
            } else if (ResourceGroup.DISABLE_RESOURCE_GROUP_NAME.equals(metric.getLabels().get(0).getValue())) {
                Assert.assertEquals(Long.valueOf(3L), metric.getValue());
            } else {
                Assert.fail();
            }
        }

        ResourceGroupMetricMgr.visitQueryLatency();

        for (Metric resourceGroupMetric : metricsResourceGroupLatency) {
            GaugeMetricImpl<Double> metric = (GaugeMetricImpl<Double>) resourceGroupMetric;
            if (wg1.getName().equals(metric.getLabels().get(1).getValue())) {
                Assert.assertEquals(Double.valueOf(10d), Double.valueOf(String.valueOf(metric.getValue())));
            } else if (wg2.getName().equals(metric.getLabels().get(1).getValue())) {
                Assert.assertEquals(Double.valueOf(10d), Double.valueOf(String.valueOf(metric.getValue())));
            } else if (ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME.equals(metric.getLabels().get(1).getValue())) {
                Assert.assertEquals(Double.valueOf(20d), Double.valueOf(String.valueOf(metric.getValue())));
            } else if (ResourceGroup.DISABLE_RESOURCE_GROUP_NAME.equals(metric.getLabels().get(1).getValue())) {
                Assert.assertEquals(Double.valueOf(30d), Double.valueOf(String.valueOf(metric.getValue())));
            } else {
                Assert.fail();
            }
        }
    }
}
