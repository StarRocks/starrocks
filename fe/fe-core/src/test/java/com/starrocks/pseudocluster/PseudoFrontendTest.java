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

package com.starrocks.pseudocluster;

import com.starrocks.common.Config;
import com.starrocks.mysql.MysqlServer;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class PseudoFrontendTest {
    @AfterAll
    public static void tearDown() {
        PseudoCluster instance = PseudoCluster.getInstance();
        if (instance != null) {
            instance.shutdown();
        }
    }

    @Test
    public void testQueryPortConflictAutoRetrySuccess() throws Exception {
        // first attempt fails, second attempt succeeds
        AtomicInteger mockFails = new AtomicInteger(1);
        new MockUp<MysqlServer>() {
            @Mock
            public boolean start(Invocation invocation) throws Exception {
                if (mockFails.decrementAndGet() >= 0) {
                    return false;
                } else {
                    Boolean result = invocation.proceed();
                    return result != null && result;
                }
            }
        };

        int queryPort = UtFrameUtils.findValidPort();
        boolean fakeJournal = true;
        int numBackends = 1;
        PseudoCluster.getOrCreate("pseudo_cluster_" + queryPort, fakeJournal, queryPort, numBackends);
        int actualQueryPort = Config.query_port;
        Assertions.assertNotEquals(queryPort, actualQueryPort);
    }
}
