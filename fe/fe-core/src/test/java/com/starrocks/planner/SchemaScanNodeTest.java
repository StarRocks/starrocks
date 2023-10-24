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

package com.starrocks.planner;

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Frontend;
import java.util.ArrayList;
import java.util.List;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class SchemaScanNodeTest {
    @Ignore
    @Test
    public void testComputeFeNodes(@Mocked GlobalStateMgr globalStateMgr) {
        List<Frontend> frontends = new ArrayList<>();
        frontends.add(new Frontend());
        Frontend frontend = new Frontend();
        frontend.setAlive(true);
        frontends.add(frontend);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
                globalStateMgr.getFrontends(null);
                minTimes = 0;
                result = frontends;
            }
        };
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        SystemTable table = new SystemTable(0, "fe_metrics", null, null, null);
        desc.setTable(table);
        SchemaScanNode scanNode = new SchemaScanNode(new PlanNodeId(0), desc);

        scanNode.computeFeNodes();

        Assert.assertNotNull(scanNode.getFrontends());
    }
}
