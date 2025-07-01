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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/loadv2/SparkLauncherMonitorTest.java

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

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class SparkLauncherMonitorTest {
    private String appId;
    private SparkLoadAppHandle.State state;
    private String queue;
    private long startTime;
    private FinalApplicationStatus finalApplicationStatus;
    private String trackingUrl;
    private String user;
    private String logPath;

    @BeforeEach
    public void setUp() {
        appId = "application_1573630236805_6864759";
        state = SparkLoadAppHandle.State.RUNNING;
        queue = "spark-queue";
        startTime = 1597916263384L;
        finalApplicationStatus = FinalApplicationStatus.UNDEFINED;
        trackingUrl = "http://myhost:8388/proxy/application_1573630236805_6864759/";
        user = "testugi";
        logPath = "./spark-launcher.log";
    }

    @Test
    public void testLogMonitorNormal() {
        URL log = getClass().getClassLoader().getResource("spark_launcher_monitor.log");
        String cmd = "cat " + log.getPath();
        SparkLoadAppHandle handle = null;
        try {
            Process process = Runtime.getRuntime().exec(cmd);
            handle = new SparkLoadAppHandle(process);
            SparkLauncherMonitor.LogMonitor logMonitor = SparkLauncherMonitor.createLogMonitor(handle);
            logMonitor.setRedirectLogPath(logPath);
            logMonitor.start();
            try {
                logMonitor.join();
            } catch (InterruptedException e) {
            }
        } catch (IOException e) {
            Assertions.fail();
        }

        // check values
        Assertions.assertEquals(appId, handle.getAppId());
        Assertions.assertEquals(state, handle.getState());
        Assertions.assertEquals(queue, handle.getQueue());
        Assertions.assertEquals(startTime, handle.getStartTime());
        Assertions.assertEquals(finalApplicationStatus, handle.getFinalStatus());
        Assertions.assertEquals(trackingUrl, handle.getUrl());
        Assertions.assertEquals(user, handle.getUser());

        // check log
        File file = new File(logPath);
        Assertions.assertTrue(file.exists());
    }

    @AfterEach
    public void clear() {
        File file = new File(logPath);
        if (file.exists()) {
            file.delete();
        }
    }
}
