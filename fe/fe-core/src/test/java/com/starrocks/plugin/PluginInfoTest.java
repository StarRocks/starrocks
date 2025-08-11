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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/plugin/PluginInfoTest.java

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

package com.starrocks.plugin;

import com.starrocks.catalog.FakeGlobalStateMgr;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.DigitalVersion;
import com.starrocks.plugin.PluginInfo.PluginType;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PluginInfoTest {
    private GlobalStateMgr globalStateMgr;

    private FakeGlobalStateMgr fakeGlobalStateMgr;

    @BeforeEach
    public void setUp() {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);

        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        FakeGlobalStateMgr.setMetaVersion(FeConstants.META_VERSION);
    }

    @Test
    public void testPluginRead() {
        try {
            PluginInfo info = PluginInfo.readFromProperties(PluginTestUtil.getTestPath("source"),
                    "test");

            assertEquals("plugin_test", info.getName());
            assertEquals(PluginType.STORAGE, info.getType());
            assertTrue(DigitalVersion.CURRENT_STARROCKS_VERSION.onOrAfter(info.getVersion()));
            assertTrue(DigitalVersion.JDK_9_0_0.onOrAfter(info.getJavaVersion()));
            assertTrue(DigitalVersion.JDK_1_8_0.before(info.getJavaVersion()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
