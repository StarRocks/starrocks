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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkClientLogHelper {
    private static final Logger LOG = LogManager.getLogger(SparkClientLogHelper.class);

    public static final String STATE = "state:";
    public static final String QUEUE = "queue:";
    public static final String START_TIME = "start time:";
    public static final String FINAL_STATUS = "final status:";
    public static final String URL = "tracking URL:";
    public static final String USER = "user:";

    // e.g.
    // input: "final status: SUCCEEDED"
    // output: "SUCCEEDED"
    public static String getValue(String line) {
        String result = null;
        List<String> entry = Splitter.onPattern(":").trimResults().limit(2).splitToList(line);
        if (entry.size() == 2) {
            result = entry.get(1);
        }
        return result;
    }

    // e.g.
    // input: "Application report for application_1573630236805_6864759 (state: ACCEPTED)"
    // output: "ACCEPTED"
    public static String regexGetState(String line) {
        String result = null;
        Matcher stateMatcher = Pattern.compile("(?<=\\(state: )(.+?)(?=\\))").matcher(line);
        if (stateMatcher.find()) {
            result = stateMatcher.group();
        }
        return result;
    }

    // e.g.
    // input: "Application report for application_1573630236805_6864759 (state: ACCEPTED)"
    // output: "application_1573630236805_6864759"
    public static String regexGetAppId(String line) {
        String result = null;
        Matcher appIdMatcher = Pattern.compile("application_[0-9]+_[0-9]+").matcher(line);
        if (appIdMatcher.find()) {
            result = appIdMatcher.group();
        }
        return result;
    }

    public static SparkLoadAppHandle.State fromYarnState(YarnApplicationState yarnState) {
        switch (yarnState) {
            case SUBMITTED:
            case ACCEPTED:
                return SparkLoadAppHandle.State.SUBMITTED;
            case RUNNING:
                return SparkLoadAppHandle.State.RUNNING;
            case FINISHED:
                return SparkLoadAppHandle.State.FINISHED;
            case FAILED:
                return SparkLoadAppHandle.State.FAILED;
            case KILLED:
                return SparkLoadAppHandle.State.KILLED;
            default:
                // NEW NEW_SAVING
                return SparkLoadAppHandle.State.UNKNOWN;
        }
    }

    public static void readLine4State(String line, SparkLoadAppHandle sparkLoadAppHandle) {
        SparkLoadAppHandle.State oldState = sparkLoadAppHandle.getState();
        SparkLoadAppHandle.State newState = oldState;
        if (line.contains(STATE)) {
            // 1. state
            String state = SparkClientLogHelper.regexGetState(line);
            if (state != null) {
                YarnApplicationState yarnState = YarnApplicationState.valueOf(state);
                newState = SparkClientLogHelper.fromYarnState(yarnState);
                if (newState != oldState) {
                    sparkLoadAppHandle.setState(newState);
                }
            }
            // 2. appId
            String appId = SparkClientLogHelper.regexGetAppId(line);
            if (appId != null) {
                if (!appId.equals(sparkLoadAppHandle.getAppId())) {
                    sparkLoadAppHandle.setAppId(appId);
                }
            }
        }
    }

    public static boolean checkLineFinalContain(String line) {
        return line.contains(QUEUE)
                || line.contains(START_TIME)
                || line.contains(FINAL_STATUS)
                || line.contains(URL)
                || line.contains(USER);
    }

    public static void readLine4OtherValues(String line, SparkLoadAppHandle sparkLoadAppHandle) {
        if (checkLineFinalContain(line)) {
            String value = getValue(line);
            if (!Strings.isNullOrEmpty(value)) {
                try {
                    if (line.contains(QUEUE)) {
                        sparkLoadAppHandle.setQueue(value);
                    } else if (line.contains(START_TIME)) {
                        sparkLoadAppHandle.setStartTime(Long.parseLong(value));
                    } else if (line.contains(FINAL_STATUS)) {
                        sparkLoadAppHandle.setFinalStatus(FinalApplicationStatus.valueOf(value));
                    } else if (line.contains(URL)) {
                        sparkLoadAppHandle.setUrl(value);
                    } else if (line.contains(USER)) {
                        sparkLoadAppHandle.setUser(value);
                    }
                } catch (IllegalArgumentException e) {
                    LOG.warn("parse log encounter an error, line: {}, msg: {}", line, e.getMessage());
                }
            }
        }
    }
}
