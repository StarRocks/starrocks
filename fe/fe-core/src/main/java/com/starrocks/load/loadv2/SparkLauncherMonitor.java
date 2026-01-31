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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/SparkLauncherMonitor.java

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

package com.starrocks.load.loadv2;

import com.google.common.base.Preconditions;
import com.starrocks.common.Config;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class SparkLauncherMonitor {
    private static final Logger LOG = LogManager.getLogger(SparkLauncherMonitor.class);

    public static LogMonitor createLogMonitor(SparkLoadAppHandle handle) {
        return new LogMonitor(handle);
    }

    // This monitor is use for monitoring the spark launcher process.
    // User can use this monitor to get real-time `appId`, `state` and `tracking-url`
    // of spark launcher by reading and analyze the output of process.
    public static class LogMonitor extends Thread {
        private final Process process;
        private SparkLoadAppHandle handle;
        private long submitTimeoutMs  = Config.spark_load_submit_timeout_second * 1000;
        private boolean isStop;

        public LogMonitor(SparkLoadAppHandle handle) {
            this.handle = handle;
            this.process = handle.getProcess();
            this.isStop = false;
        }

        // Seconds turn into milliseconds
        public void setSubmitTimeoutMs(long submitTimeout) {
            this.submitTimeoutMs = submitTimeout * 1000;
        }


        // Normally, log monitor will automatically stop if the spark app state changes
        // to RUNNING.
        // But if the spark app state changes to FAILED/KILLED/LOST, log monitor will stop
        // and kill the spark launcher process.
        // There is a `submitTimeout` for preventing the spark app state from staying in
        // UNKNOWN/SUBMITTED for a long time.
        @Override
        public void run() {
            if (handle.getState() == SparkLoadAppHandle.State.KILLED) {
                // If handle has been killed, kill the process
                process.destroyForcibly();
                return;
            }
            // monitor spark client log
            BufferedReader outReader = null;
            String line = null;
            long startTime = System.currentTimeMillis();
            try {
                // don't close out reader,spark client log need be persisted to local disk
                outReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                while (!isStop && (line = outReader.readLine()) != null) {
                    SparkLoadAppHandle.State oldState = handle.getState();
                    SparkLoadAppHandle.State newState = oldState;
                    // parse state and appId
                    if (line.contains(SparkClientLogHelper.STATE)) {
                        // 1. state
                        String state = SparkClientLogHelper.regexGetState(line);
                        if (state != null) {
                            YarnApplicationState yarnState = YarnApplicationState.valueOf(state);
                            newState = SparkClientLogHelper.fromYarnState(yarnState);
                            if (newState != oldState) {
                                handle.setState(newState);
                            }
                        }
                        // 2. appId
                        String appId = SparkClientLogHelper.regexGetAppId(line);
                        if (appId != null) {
                            if (!appId.equals(handle.getAppId())) {
                                handle.setAppId(appId);
                            }
                        }

                        LOG.debug("spark appId that handle get is {}, state: {}", handle.getAppId(),
                                handle.getState().toString());
                        switch (newState) {
                            case UNKNOWN:
                            case CONNECTED:
                            case SUBMITTED:
                                // If the app stays in the UNKNOWN/CONNECTED/SUBMITTED state for more than submitTimeoutMs
                                // stop monitoring and kill the process
                                if (System.currentTimeMillis() - startTime > submitTimeoutMs) {
                                    isStop = true;
                                    handle.kill();
                                }
                                break;
                            case RUNNING:
                            case FINISHED:
                                // There's no need to parse all logs of handle process to get all the information.
                                // As soon as the state changes to RUNNING/FINISHED,
                                // stop monitoring but keep the process alive.
                                isStop = true;
                                break;
                            case KILLED:
                            case FAILED:
                            case LOST:
                                // If the state changes to KILLED/FAILED/LOST,
                                // stop monitoring and kill the process
                                isStop = true;
                                handle.kill();
                                break;
                            default:
                                Preconditions.checkState(false, "wrong spark app state");
                        }
                    }
                    // parse other values
                    SparkClientLogHelper.readLine4OtherValues(line, handle);
                }
            } catch (Exception e) {
                LOG.warn("Exception monitoring process.", e);
            }
            // monitor finish
            if (isStop) {
                // if process is kill,close the buffer read
                if (handle.getProcess() == null) {
                    try {
                        if (outReader != null) {
                            outReader.close();
                        }
                    } catch (IOException e) {
                        LOG.warn("close buffered reader error", e);
                    }
                } else {
                    // When the monitor thread exits, the handle is handed over to SparkClientLogParser
                    try {
                        SparkClientLogParser sparkClientLogParser = SparkClientLogParser.createLogParser(handle);
                        sparkClientLogParser.start();
                    } catch (Exception e) {
                        LOG.warn("rewrite spark client log fail.", e);
                    }
                }
            }
        }


    }
}
