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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/MasterDaemon.java

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

package com.starrocks.common.util;

import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
 * FrontendDaemon will wait for FE to be ready before running.
 */
public class FrontendDaemon extends Daemon {
    private static final Logger LOG = LogManager.getLogger(FrontendDaemon.class);

    public FrontendDaemon() {
    }

    public FrontendDaemon(String name) {
        super(name);
    }

    public FrontendDaemon(String name, long intervalMs) {
        super(name, intervalMs);
    }

    @Override
    protected void runOneCycle() {
        while (!GlobalStateMgr.getServingState().isReady()) {
            // here we use getServingState(), not getCurrentState() because we truly want the GlobalStateMgr instance,
            // not the Checkpoint globalStateMgr instance.
            // and if globalStateMgr is not ready, do not run
            try {
                // not return, but sleep a while. to avoid some thread with large running interval will
                // wait for a long time to start again.
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOG.warn("interrupted exception. thread: {}", getName(), e);
            }
        }
        runAfterCatalogReady();
    }

    // override by derived classes
    protected void runAfterCatalogReady() {

    }
}
