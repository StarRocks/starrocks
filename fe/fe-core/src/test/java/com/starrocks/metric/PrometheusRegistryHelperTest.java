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

import com.starrocks.common.ExceptionChecker;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Summary;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class PrometheusRegistryHelperTest {
    private static final Logger LOG = LogManager.getLogger(PrometheusRegistryHelperTest.class);

    @Test
    public void testExceptionConvertingPrometheusMetrics() {
        PrometheusRegistry registry = new PrometheusRegistry();
        Summary summary = Summary.builder().name("summary_metric").register(registry);
        Gauge gauge = Gauge.builder().name("gauge_metric").register(registry);
        summary.observe(1);
        gauge.set(10);

        MetricVisitor visitor = new PrometheusMetricVisitor("fe_ut");
        ExceptionChecker.expectThrowsNoException(
                () -> PrometheusRegistryHelper.visitPrometheusRegistry(registry, visitor));
        String result = visitor.build();
        LOG.warn("MetricVisitor produces: {}", visitor.build());
        Assert.assertTrue(result.contains("fe_ut_gauge_metric"));
        Assert.assertFalse(result.contains("fe_ut_summary_metric"));
    }
}
