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


package com.starrocks.load.loadv2;

import com.starrocks.common.LoadException;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class YarnApplicationReportTest {
    private final String runningReport = "Application Report :\n" +
            "Application-Id : application_15888888888_0088\n" +
            "Application-Name : label0\n" +
            "Application-Type : SPARK-2.4.1\n" +
            "User : test\n" +
            "Queue : test-queue\n" +
            "Start-Time : 1597654469958\n" +
            "Finish-Time : 0\n" +
            "Progress : 50%\n" +
            "State : RUNNING\n" +
            "Final-State : UNDEFINED\n" +
            "Tracking-URL : http://127.0.0.1:8080/proxy/application_1586619723848_0088/\n" +
            "RPC Port : 40236\n" +
            "AM Host : host-name";

    @Test
    public void testParseToReport() {
        try {
            YarnApplicationReport yarnReport = new YarnApplicationReport(runningReport);
            ApplicationReport report = yarnReport.getReport();
            Assertions.assertEquals("application_15888888888_0088", report.getApplicationId().toString());
            Assertions.assertEquals("label0", report.getName());
            Assertions.assertEquals("test", report.getUser());
            Assertions.assertEquals("test-queue", report.getQueue());
            Assertions.assertEquals(1597654469958L, report.getStartTime());
            Assertions.assertEquals(0L, report.getFinishTime());
            Assertions.assertTrue(report.getProgress() == 0.5f);
            Assertions.assertEquals(YarnApplicationState.RUNNING, report.getYarnApplicationState());
            Assertions.assertEquals(FinalApplicationStatus.UNDEFINED, report.getFinalApplicationStatus());
            Assertions.assertEquals("http://127.0.0.1:8080/proxy/application_1586619723848_0088/", report.getTrackingUrl());
            Assertions.assertEquals(40236, report.getRpcPort());
            Assertions.assertEquals("host-name", report.getHost());

        } catch (LoadException e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }
}
