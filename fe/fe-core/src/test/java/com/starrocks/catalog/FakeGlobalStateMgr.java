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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/FakeCatalog.java

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

package com.starrocks.catalog;

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;

public class FakeGlobalStateMgr extends MockUp<GlobalStateMgr> {

    private static GlobalStateMgr globalStateMgr;
    private static int metaVersion;
    private static NodeMgr nodeMgr = new NodeMgr();
    private static SystemInfoService systemInfo = new SystemInfoService();

    public static void setGlobalStateMgr(GlobalStateMgr globalStateMgr) {
        FakeGlobalStateMgr.globalStateMgr = globalStateMgr;
    }

    public static void setMetaVersion(int metaVersion) {
        FakeGlobalStateMgr.metaVersion = metaVersion;
    }

    public static void setSystemInfo(SystemInfoService systemInfo) {
        FakeGlobalStateMgr.systemInfo = systemInfo;
    }

    @Mock
    public static GlobalStateMgr getCurrentState() {
        return globalStateMgr;
    }

    @Mock
    public static GlobalStateMgr getInstance() {
        return globalStateMgr;
    }

    @Mock
    public static int getCurrentStateJournalVersion() {
        return metaVersion;
    }

    @Mock
    public static NodeMgr getNodeMgr() {
        return nodeMgr;
    }

    @Mock
    public static SystemInfoService getClusterInfo() {
        return systemInfo;
    }

}