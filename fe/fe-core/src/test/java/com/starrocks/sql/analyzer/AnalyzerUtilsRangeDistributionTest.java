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
 * {@link AnalyzerUtils#isEnableRangeDistribution(ConnectContext)}.
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
        mockRunMode();
        try {
            // 8 combinations with a non-null context carrying the session variable.
            for (boolean config : new boolean[] {false, true}) {
                for (boolean shared : new boolean[] {false, true}) {
                    for (boolean sessionVar : new boolean[] {false, true}) {
                        Config.enable_range_distribution = config;
                        sharedData[0] = shared;
                        ConnectContext ctx = new ConnectContext();
                        ctx.getSessionVariable().setEnableRangeDistribution(sessionVar);

                        boolean expected = (config && shared) || sessionVar;
                        assertEquals(expected,
                                AnalyzerUtils.isEnableRangeDistribution(ctx),
                                "config=" + config + " sharedData=" + shared + " sessionVar=" + sessionVar);
                    }
                }
            }

            // 4 additional {config x mode} checks with a null context: only the
            // config-driven shared-data default can apply (no session variable to read).
            for (boolean config : new boolean[] {false, true}) {
                for (boolean shared : new boolean[] {false, true}) {
                    Config.enable_range_distribution = config;
                    sharedData[0] = shared;

                    boolean expected = config && shared;
                    assertEquals(expected,
                            AnalyzerUtils.isEnableRangeDistribution(null),
                            "nullCtx config=" + config + " sharedData=" + shared);
                }
            }
        } finally {
            Config.enable_range_distribution = savedConfig;
        }
    }
}
