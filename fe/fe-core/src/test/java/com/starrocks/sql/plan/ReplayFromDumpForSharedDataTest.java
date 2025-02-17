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

package com.starrocks.sql.plan;

import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.RunMode;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReplayFromDumpForSharedDataTest extends ReplayFromDumpTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.show_execution_groups = false;
        // Should disable Dynamic Partition in replay dump test
        Config.dynamic_partition_enable = false;
        Config.tablet_sched_disable_colocate_overall_balance = true;
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);
        // set default config for timeliness mvs
        UtFrameUtils.mockTimelinessForAsyncMVTest(connectContext);

        new MockUp<EditLog>() {
            @Mock
            protected void logEdit(short op, Writable writable) {
                return;
            }
        };
    }

    @Test
    public void testReplicationNum() throws Exception {
        String dumpInfo = getDumpInfoFromFile("query_dump/shared_data_query_test");
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dumpInfo);
        SessionVariable sessionVariable = queryDumpInfo.getSessionVariable();
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(dumpInfo, sessionVariable);
        Assert.assertTrue(replayPair.second, replayPair.second.contains("mv_name_1"));
    }
}
