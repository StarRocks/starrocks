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

package com.starrocks.server;

import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RunMode {
    public static final RunMode SHARED_NOTHING = new RunMode("shared_nothing", true, false, false);
    public static final RunMode SHARED_DATA = new RunMode("shared_data", false, true, true);
    public static final RunMode HYBRID = new RunMode("experimental_hybrid", true, true, false);
    private static final Logger LOG = LogManager.getLogger(RunMode.class);
    private static RunMode currentRunMode = SHARED_NOTHING;

    private final String name;
    private final boolean allowCreateOlapTable;
    private final boolean allowCreateLakeTable;
    private final boolean ignoreReplicationNum;

    private RunMode(String name, boolean allowCreateOlapTable, boolean allowCreateLakeTable, boolean ignoreReplicationNum) {
        this.name = name;
        this.allowCreateOlapTable = allowCreateOlapTable;
        this.allowCreateLakeTable = allowCreateLakeTable;
        this.ignoreReplicationNum = ignoreReplicationNum;
    }

    public static void detectRunMode() {
        String runMode = Config.run_mode;
        if (SHARED_NOTHING.getName().equalsIgnoreCase(runMode)) {
            currentRunMode = SHARED_NOTHING;
        } else if (SHARED_DATA.getName().equalsIgnoreCase(runMode)) {
            currentRunMode = SHARED_DATA;
        } else if (HYBRID.getName().equalsIgnoreCase(runMode)) {
            LOG.warn("The run mode \"{}\" is not production ready, should only be used in test environment", HYBRID);
            currentRunMode = HYBRID;
        } else {
            LOG.error("Invalid run_mode \"{}\". The candidates are \"{}\" and \"{}\"", runMode, SHARED_NOTHING, SHARED_DATA);
            System.exit(-1);
        }
    }

    public static RunMode getCurrentRunMode() {
        return currentRunMode;
    }

    public String getName() {
        return name;
    }

    public boolean isAllowCreateOlapTable() {
        return allowCreateOlapTable;
    }

    public boolean isAllowCreateLakeTable() {
        return allowCreateLakeTable;
    }

    public boolean isIgnoreReplicationNum() {
        return ignoreReplicationNum;
    }

    @Override
    public String toString() {
        return getName();
    }
}
