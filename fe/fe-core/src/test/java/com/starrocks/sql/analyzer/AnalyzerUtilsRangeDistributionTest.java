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

package com.starrocks.sql.analyzer;

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Cluster-free coverage of the {@code (config && shared-data) || session-var} policy in
 * {@link AnalyzerUtils#isEnableRangeDistribution(ConnectContext)}, and of the extra
 * {@code enable_mv_range_distribution} gate that
 * {@link AnalyzerUtils#isEnableMvRangeDistribution(ConnectContext)} puts on its config-driven half.
 */
public class AnalyzerUtilsRangeDistributionTest {

    // Mutable flag read by the mocked RunMode.getCurrentRunMode(), toggled per case.
    private final boolean[] sharedData = {false};

    private void mockRunMode() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return sharedData[0] ? RunMode.SHARED_DATA : RunMode.SHARED_NOTHING;
            }
        };
    }

    @Test
    public void testPredicateMatrix() {
        boolean savedConfig = Config.enable_range_distribution;
        boolean savedMvConfig = Config.enable_mv_range_distribution;
        mockRunMode();
        try {
            // Every {config x mvConfig x mode x session} combination, with a non-null context
            // carrying the session variable.
            for (boolean config : new boolean[] {false, true}) {
                for (boolean mvConfig : new boolean[] {false, true}) {
                    for (boolean shared : new boolean[] {false, true}) {
                        for (boolean sessionVar : new boolean[] {false, true}) {
                            Config.enable_range_distribution = config;
                            Config.enable_mv_range_distribution = mvConfig;
                            sharedData[0] = shared;
                            ConnectContext ctx = new ConnectContext();
                            ctx.getSessionVariable().setEnableRangeDistribution(sessionVar);
                            String combination = "config=" + config + " mvConfig=" + mvConfig
                                    + " sharedData=" + shared + " sessionVar=" + sessionVar;

                            assertEquals((config && shared) || sessionVar,
                                    AnalyzerUtils.isEnableRangeDistribution(ctx), combination);
                            // A table stays range-distributed while its materialized views do not,
                            // unless the session opts in.
                            assertEquals((mvConfig && config && shared) || sessionVar,
                                    AnalyzerUtils.isEnableMvRangeDistribution(ctx), combination);
                        }
                    }
                }
            }

            // The same {config x mvConfig x mode} combinations with a null context: only the
            // config-driven shared-data default can apply, as there is no session variable to read.
            for (boolean config : new boolean[] {false, true}) {
                for (boolean mvConfig : new boolean[] {false, true}) {
                    for (boolean shared : new boolean[] {false, true}) {
                        Config.enable_range_distribution = config;
                        Config.enable_mv_range_distribution = mvConfig;
                        sharedData[0] = shared;
                        String combination = "nullCtx config=" + config + " mvConfig=" + mvConfig
                                + " sharedData=" + shared;

                        assertEquals(config && shared,
                                AnalyzerUtils.isEnableRangeDistribution(null), combination);
                        assertEquals(mvConfig && config && shared,
                                AnalyzerUtils.isEnableMvRangeDistribution(null), combination);
                    }
                }
            }
        } finally {
            Config.enable_range_distribution = savedConfig;
            Config.enable_mv_range_distribution = savedMvConfig;
        }
    }
}
