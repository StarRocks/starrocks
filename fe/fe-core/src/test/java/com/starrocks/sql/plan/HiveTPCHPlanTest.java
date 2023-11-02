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

import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HiveTPCHPlanTest extends ConnectorPlanTestBase {
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @BeforeClass
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.doInit(temp.newFolder().toURI().toString());
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        GlobalStateMgr.getCurrentState().changeCatalogDb(connectContext, "hive0.tpch");
    }

    @AfterClass
    public static void afterClass() {
        try {
            UtFrameUtils.dropMockBackend(10002);
            UtFrameUtils.dropMockBackend(10003);
        } catch (DdlException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTPCH1() {
        runFileUnitTest("external/hive/tpch/q1");
    }

    @Test
    public void testTPCH2() {
        runFileUnitTest("external/hive/tpch/q2");
    }

    @Test
    public void testTPCH3() {
        runFileUnitTest("external/hive/tpch/q3");
    }

    @Test
    public void testTPCH4() {
        runFileUnitTest("external/hive/tpch/q4");
    }

    @Test
    public void testTPCH5() {
        runFileUnitTest("external/hive/tpch/q5");
    }

    @Test
    public void testTPCH6() {
        runFileUnitTest("external/hive/tpch/q6");
    }

    @Test
    public void testTPCH7() {
        runFileUnitTest("external/hive/tpch/q7");
    }

    @Test
    public void testTPCH8() {
        int oldValue = connectContext.getSessionVariable().getMaxTransformReorderJoins();
        connectContext.getSessionVariable().setMaxTransformReorderJoins(4);
        runFileUnitTest("external/hive/tpch/q8");
        connectContext.getSessionVariable().setMaxTransformReorderJoins(oldValue);
    }

    @Test
    public void testTPCH9() {
        runFileUnitTest("external/hive/tpch/q9");
    }

    @Test
    public void testTPCH10() {
        runFileUnitTest("external/hive/tpch/q10");
    }

    @Test
    public void testTPCH11() {
        runFileUnitTest("external/hive/tpch/q11", true);
    }

    @Test
    public void testTPCH12() {
        runFileUnitTest("external/hive/tpch/q12");
    }

    @Test
    public void testTPCH13() {
        runFileUnitTest("external/hive/tpch/q13", true);
    }

    @Test
    public void testTPCH14() {
        runFileUnitTest("external/hive/tpch/q14");
    }

    @Test
    public void testTPCH15() {
        runFileUnitTest("external/hive/tpch/q15");
    }

    @Test
    public void testTPCH16() {
        runFileUnitTest("external/hive/tpch/q16");
    }

    @Test
    public void testTPCH17() {
        runFileUnitTest("external/hive/tpch/q17");
    }

    @Test
    public void testTPCH18() {
        runFileUnitTest("external/hive/tpch/q18");
    }

    @Test
    public void testTPCH19() {
        runFileUnitTest("external/hive/tpch/q19");
    }

    @Test
    public void testTPCH20() {
        runFileUnitTest("external/hive/tpch/q20");
    }

    @Test
    public void testTPCH21() {
        runFileUnitTest("external/hive/tpch/q21");
    }

    @Test
    public void testTPCH22() {
        runFileUnitTest("external/hive/tpch/q22", true);
    }
}