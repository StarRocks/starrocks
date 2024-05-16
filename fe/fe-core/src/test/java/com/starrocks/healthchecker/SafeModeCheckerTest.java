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

package com.starrocks.healthchecker;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SafeModeCheckerTest {

    private SafeModeChecker safeModeChecker = new SafeModeChecker();
    @Mocked GlobalStateMgr globalStateMgr;

    Backend be = new Backend(-1000L, "", 0);

    @Before
    public void setUp() {
        globalStateMgr.getNodeMgr().getClusterInfo().addBackend(be);
        ImmutableMap<String, DiskInfo> disksRef;
        DiskInfo diskInfo = new DiskInfo("");
        diskInfo.setTotalCapacityB(100);
        diskInfo.setAvailableCapacityB(5);
        disksRef = ImmutableMap.of("", diskInfo);
        be.setDisks(disksRef);
        be.setAlive(true);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends();
                minTimes = 0;
                result = be;
            }
        };
    }

    @After
    public void tearDown() throws DdlException {
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropBackend(be);
        GlobalStateMgr.getCurrentState().setSafeMode(false);
    }

    @Test
    public void testSafeMode() {
        assertEquals(true, safeModeChecker.checkInternal());
    }
}
