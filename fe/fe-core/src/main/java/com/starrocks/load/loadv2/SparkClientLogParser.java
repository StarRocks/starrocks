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

import com.starrocks.common.LoadException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class SparkClientLogParser extends Thread {
    private static final Logger LOG = LogManager.getLogger(SparkClientLogParser.class);
    private SparkLoadAppHandle sparkLoadAppHandle;

    public static SparkClientLogParser createLogParser(SparkLoadAppHandle handle) throws IOException {
        return new SparkClientLogParser(handle);
    }

    public SparkClientLogParser(SparkLoadAppHandle handle) throws IOException {
        this.sparkLoadAppHandle = handle;
    }

    @Override
    public void run() {
        try (BufferedReader outReader =
                    new BufferedReader(new InputStreamReader(sparkLoadAppHandle.getProcess().getInputStream()));
                BufferedWriter fileWriter =
                    new BufferedWriter(new FileWriter(sparkLoadAppHandle.getLogPath()))) {
            String line;
            Set<String> stateUniqueSet = new HashSet<>();
            while ((line = outReader.readLine()) != null) {
                if (SparkClientLogHelper.checkLineFinalContain(line)) {
                    fileWriter.write(line);
                    fileWriter.newLine();
                }
                // get spark state
                if (line.contains(SparkClientLogHelper.STATE)) {
                    String state = SparkClientLogHelper.regexGetState(line);
                    if (state == null || state.isEmpty()) {
                        continue;
                    }
                    // filter line if contain duplicate states
                    if (!stateUniqueSet.contains(state)) {
                        fileWriter.write(line);
                        fileWriter.newLine();
                    }
                    stateUniqueSet.add(state);
                }
            }
        } catch (IOException e) {
            LOG.warn("Exception spark client log process.", e);
        }
    }

    public void readLog() throws LoadException {
        String filePath = sparkLoadAppHandle.getLogPath();
        if (filePath == null || filePath.isEmpty()) {
            throw new LoadException("can not get log path from spark load app handler");
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                SparkClientLogHelper.readLine4State(line, sparkLoadAppHandle);
                SparkClientLogHelper.readLine4OtherValues(line, sparkLoadAppHandle);
            }
        } catch (IOException e) {
            LOG.error("Failed to read the log file: " + e.getMessage());
        }
    }

    public boolean checkSparkAbnormal() {
        boolean abnormal = true;
        String appId = sparkLoadAppHandle.getAppId();
        String state = sparkLoadAppHandle.getState().toString();
        if (appId != null && !appId.isEmpty() && state != null && !state.isEmpty()) {
            abnormal = false;
        }
        return abnormal;
    }
}
