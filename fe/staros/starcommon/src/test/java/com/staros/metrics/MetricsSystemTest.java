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

package com.staros.metrics;

import com.google.common.collect.Lists;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MetricsSystemTest {
    private Counter counter = null;
    private Gauge gauge = null;

    @After
    public void tearDown() {
        if (counter != null) {
            MetricsSystem.removeCollector(counter);
        }
        if (gauge != null) {
            MetricsSystem.removeCollector(gauge);
        }
    }

    private boolean findLineFromMultipleLine(String expectedStr, String content) {
        String [] lines = content.split("\n");
        for (String line : lines) {
            if (expectedStr.equals(line)) {
                return true;
            }
        }
        return false;
    }

    private String metricsExportToString() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        MetricsSystem.exportMetricsTextFormat(outputStream);
        return outputStream.toString();
    }

    @Test
    public void testCounterWithoutLabel() throws IOException {
        counter = MetricsSystem.registerCounter("counter_with_no_label", "help");
        counter.inc();

        {
            String content = metricsExportToString();
            String expectedStr = "counter_with_no_label_total 1.0";
            Assert.assertTrue(content, findLineFromMultipleLine(expectedStr, content));
        }

        counter.inc();
        {
            String content = metricsExportToString();
            String expectedStr = "counter_with_no_label_total 2.0";
            Assert.assertTrue(content, findLineFromMultipleLine(expectedStr, content));
        }
    }

    @Test
    public void testCounterWithLabels() throws IOException {
        counter = MetricsSystem.registerCounter("counter_with_label_x_y", "help", Lists.newArrayList("xlabel", "ylabel"));
        counter.labelValues("x1", "y1").inc();

        {
            String content = metricsExportToString();
            String expectedStr = "counter_with_label_x_y_total{xlabel=\"x1\",ylabel=\"y1\"} 1.0";
            Assert.assertTrue(content, findLineFromMultipleLine(expectedStr, content));
        }

        counter.labelValues("x2", "y2").inc();
        {
            String content = metricsExportToString();
            // expect both labels are there exported
            String expectedStr1 = "counter_with_label_x_y_total{xlabel=\"x1\",ylabel=\"y1\"} 1.0";
            Assert.assertTrue(content, findLineFromMultipleLine(expectedStr1, content));
            String expectedStr2 = "counter_with_label_x_y_total{xlabel=\"x2\",ylabel=\"y2\"} 1.0";
            Assert.assertTrue(content, findLineFromMultipleLine(expectedStr2, content));
        }
    }

    @Test
    public void testGaugeWithoutLabel() throws IOException {
        gauge = MetricsSystem.registerGauge("gauge_with_no_label", "help");
        gauge.set(1024);

        {
            String content = metricsExportToString();
            String expectedStr = "gauge_with_no_label 1024.0";
            Assert.assertTrue(content, findLineFromMultipleLine(expectedStr, content));
        }

        gauge.dec(2028);
        {
            String content = metricsExportToString();
            String expectedStr = "gauge_with_no_label -1004.0"; // 1024 - 2028
            Assert.assertTrue(content, findLineFromMultipleLine(expectedStr, content));
        }
    }

    @Test
    public void testGaugeWithLabels() throws IOException {
        gauge = MetricsSystem.registerGauge("gauge_with_label_x_y", "help", Lists.newArrayList("xlabel", "ylabel"));

        gauge.labelValues("x1", "y1").set(1048576);
        {
            String content = metricsExportToString();
            String expectedStr = "gauge_with_label_x_y{xlabel=\"x1\",ylabel=\"y1\"} 1048576.0";
            Assert.assertTrue(content, findLineFromMultipleLine(expectedStr, content));
        }

        gauge.labelValues("x2", "y2").set(-10086);
        {
            String content = metricsExportToString();
            // expect both labels are there exported
            String expectedStr1 = "gauge_with_label_x_y{xlabel=\"x1\",ylabel=\"y1\"} 1048576.0";
            Assert.assertTrue(content, findLineFromMultipleLine(expectedStr1, content));
            String expectedStr2 = "gauge_with_label_x_y{xlabel=\"x2\",ylabel=\"y2\"} -10086.0";
            Assert.assertTrue(content, findLineFromMultipleLine(expectedStr2, content));
        }
    }
}
