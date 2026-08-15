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

import com.starrocks.common.FeConstants;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Predicate;

public class ComputeNodeMetricTest {

    @Mocked
    private StarOSAgent starOSAgent;

    private static ComputeNode cn;

    private static final String CN_HOST = "192.168.1.100";
    private static final int CN_HEARTBEAT_PORT = 9050;
    private static final int CN_STARLET_PORT = 9070;

    @BeforeAll
    public static void beforeClass() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();

        cn = new ComputeNode(1234, CN_HOST, CN_HEARTBEAT_PORT);
        cn.setStarletPort(CN_STARLET_PORT);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNode(cn);
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };
    }

    @AfterAll
    public static void afterClass() {
        SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        clusterInfo.dropComputeNode(cn);
        MetricRepo.generateBackendsTabletMetrics();
    }

    @Test
    public void testComputeNodeTabletNumMetricWhenLeader() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }

            @Mock
            boolean isLeader() {
                return true;
            }
        };
        long expectedTabletNum = 42L;

        new Expectations() {
            {
                starOSAgent.getWorkerTabletNum(CN_HOST + ":" + CN_STARLET_PORT);
                result = expectedTabletNum;
            }
        };

        MetricRepo.generateBackendsTabletMetrics();

        List<Metric> tabletNumMetrics = MetricRepo.getMetricsByName(MetricRepo.TABLET_NUM);
        Assertions.assertFalse(tabletNumMetrics.isEmpty());

        String label = CN_HOST + ":" + CN_HEARTBEAT_PORT;
        Metric<Long> cnMetric = findMetricByLabel(tabletNumMetrics, label);

        Assertions.assertNotNull(cnMetric);
        Assertions.assertEquals(expectedTabletNum, (long) cnMetric.getValue());
    }

    @Test
    public void testComputeNodeTabletNumMetricReturnsZeroWhenNotLeader() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }

            @Mock
            boolean isLeader() {
                return false;
            }
        };

        MetricRepo.generateBackendsTabletMetrics();

        String label = CN_HOST + ":" + CN_HEARTBEAT_PORT;
        List<Metric> tabletNumMetrics = MetricRepo.getMetricsByName(MetricRepo.TABLET_NUM);
        Metric<Long> cnMetric = findMetricByLabel(tabletNumMetrics, label);

        Assertions.assertNotNull(cnMetric);
        Assertions.assertEquals(0L, (long) cnMetric.getValue());
    }

    @SuppressWarnings("unchecked")
    private Metric<Long> findMetricByLabel(List<Metric> metrics, String label) {
        for (Metric metric : metrics) {
            boolean matches = metric.getLabels().stream()
                    .anyMatch((Predicate<MetricLabel>) l -> "backend".equals(l.getKey()) && l.getValue().equals(label));
            if (matches) {
                return metric;
            }
        }
        return null;
    }
}
