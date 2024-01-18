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

import com.starrocks.common.conf.Config;
import com.starrocks.thrift.TRunMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class RunMode {
    public static final RunMode SHARED_NOTHING = new SharedNothing();
    public static final RunMode SHARED_DATA = new SharedData();
    private static final Logger LOG = LogManager.getLogger(RunMode.class);
    // Database and table's default configurations, we will never change them
    private static RunMode currentRunMode = SHARED_NOTHING;

    private final String name;

    private RunMode(String name) {
        this.name = name;
    }

    public static void detectRunMode() {
        String runMode = Config.run_mode;
        if (SHARED_NOTHING.getName().equalsIgnoreCase(runMode)) {
            currentRunMode = SHARED_NOTHING;
        } else if (SHARED_DATA.getName().equalsIgnoreCase(runMode)) {
            currentRunMode = SHARED_DATA;
        } else {
            LOG.error("Invalid run_mode \"{}\". The candidates are \"{}\" and \"{}\"", runMode, SHARED_NOTHING, SHARED_DATA);
            System.exit(-1);
        }
    }

    public static TRunMode toTRunMode(RunMode mode) {
        if (mode == SHARED_DATA) {
            return TRunMode.SHARED_DATA;
        } else {
            return TRunMode.SHARED_NOTHING;
        }
    }

    public static RunMode getCurrentRunMode() {
        return currentRunMode;
    }

    public static String name() {
        return getCurrentRunMode().getName();
    }

    public static short defaultReplicationNum() {
        return getCurrentRunMode().getDefaultReplicationNum();
    }

    public String getName() {
        return name;
    }

    public abstract short getDefaultReplicationNum();

    public static boolean isSharedDataMode() {
        return getCurrentRunMode() == RunMode.SHARED_DATA;
    }

    public static boolean isSharedNothingMode() {
        return getCurrentRunMode() == RunMode.SHARED_NOTHING;
    }

    @Override
    public String toString() {
        return getName();
    }

    private static class SharedNothing extends RunMode {
        private SharedNothing() {
            super("shared_nothing");
        }

        @Override
        public short getDefaultReplicationNum() {
            return Config.default_replication_num;
        }
    }

    private static class SharedData extends RunMode {
        private SharedData() {
            super("shared_data");
        }

        @Override
        public short getDefaultReplicationNum() {
            return 1;
        }
    }
}
