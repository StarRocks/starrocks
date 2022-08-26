// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.metric;

import com.starrocks.catalog.ResourceGroup;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class ResourceGroupMetricTest {

    @BeforeClass
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @Test
    public void testResourceGroupMetrics() {
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableResourceGroup(true);
        ctx.setSessionVariable(sessionVariable);

        ResourceGroup wg1 = new ResourceGroup();
        wg1.setName("wg1");
        ResourceGroup wg2 = new ResourceGroup();
        wg2.setName("wg2");

        ctx.setResourceGroup(wg1);
        ctx.getAuditEventBuilder().setResourceGroup(wg1.getName());
        ResourceGroupMetricMgr.increaseQuery(ctx, 1L);
        ResourceGroupMetricMgr.increaseQueryErr(ctx, 1L);
        ResourceGroupMetricMgr.updateQueryLatency(ctx, 10L);

        ctx.setResourceGroup(wg2);
        ctx.getAuditEventBuilder().setResourceGroup(wg2.getName());
        ResourceGroupMetricMgr.increaseQuery(ctx, 1L);
        ResourceGroupMetricMgr.increaseQueryErr(ctx, 1L);
        ResourceGroupMetricMgr.updateQueryLatency(ctx, 10L);

        List<Metric> metricsResourceGroup = MetricRepo.getMetricsByName("query_resource_group");
        List<Metric> metricsResourceGroupErr = MetricRepo.getMetricsByName("query_resource_group_err");
        List<Metric> metricsResourceGroupLatency = MetricRepo.getMetricsByName("query_resource_group_latency");

        Assert.assertEquals(2, metricsResourceGroup.size());
        Assert.assertEquals(2, metricsResourceGroupErr.size());
        Assert.assertEquals(2 * 6, metricsResourceGroupLatency.size());

        for (Metric resourceGroupMetric : metricsResourceGroup) {
            LongCounterMetric metric = (LongCounterMetric) resourceGroupMetric;
            if (wg1.getName().equals(metric.getLabels().get(0).getValue())) {
                Assert.assertEquals(Long.valueOf(1L), metric.getValue());
            } else if (wg2.getName().equals(metric.getLabels().get(0).getValue())) {
                Assert.assertEquals(Long.valueOf(1L), metric.getValue());
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
            } else {
                Assert.fail();
            }
        }
    }
}
