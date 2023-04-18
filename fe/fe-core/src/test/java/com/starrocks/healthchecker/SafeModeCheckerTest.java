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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/system/HeartbeatMgrTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.healthchecker;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import mockit.Expectations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SafeModeCheckerTest {

    private SafeModeChecker safeModeChecker = new SafeModeChecker();
    private GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

    Backend be = new Backend(-1000L, "", 0);

    @Before
    public void setUp() {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
        globalStateMgr.getClusterInfo().addBackend(be);
        ImmutableMap<String, DiskInfo> disksRef;
        DiskInfo diskInfo = new DiskInfo("");
        diskInfo.setTotalCapacityB(100);
        diskInfo.setAvailableCapacityB(5);
        disksRef = ImmutableMap.of("", diskInfo);
        be.setDisks(disksRef);
        be.setAlive(true);
    }

    @After
    public void tearDown() throws DdlException {
        GlobalStateMgr.getCurrentState().getClusterInfo().dropBackend(be);
        globalStateMgr.setSafeMode(false);
    }

    @Test
    public void testSafeMode() {
        safeModeChecker.checkInternal();
        assertEquals(true, globalStateMgr.isSafeMode());
    }
}
