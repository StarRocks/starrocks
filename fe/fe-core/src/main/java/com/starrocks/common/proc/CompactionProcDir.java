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

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.AnalysisException;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utils.RestClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

public class CompactionProcDir implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES;
    private static final Logger LOG = LogManager.getLogger(CompactionProcDir.class);

    static {
        ImmutableList.Builder<String> builder = new ImmutableList.Builder<String>()
                .add("BackendId").add("TotalRunningNum").add("BaseRunningNum").add("CumuRunningNum")
                .add("UpdateRunningNum").add("TotalWaitingNum").add("BaseWaitingNum").add("CumuWaitingNum")
                .add("RunningDetails").add("WaitingDetails");
        TITLE_NAMES = builder.build();
    }

    private SystemInfoService clusterInfoService;

    public CompactionProcDir(SystemInfoService clusterInfoService) {
        this.clusterInfoService = clusterInfoService;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(clusterInfoService);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (Long backendId : clusterInfoService.getBackendIds(false)) {
            String runningTaskUrl = "N/A";
            String waitingTaskUrl = "N/A";
            String totalRunningNum = "N/A";
            String baseRunningNum = "N/A";
            String cumuRunningNum = "N/A";
            String updateRunningNum = "N/A";
            String totalWaitingNum = "N/A";
            String baseWaitingNum = "N/A";
            String cumuWaitingNum = "N/A";
            if (backendId != null) {
                Backend backend = clusterInfoService.getBackend(backendId);
                RestClient client = new RestClient(5000);
                String res;
                try {
                    res = client.httpGet(String.format(
                            "http://%s:%d/api/compaction/show_num",
                            backend.getHost(),
                            backend.getHttpPort()));
                    String[] taskNumArr = res.split(" ");
                    if (taskNumArr.length == 7) {
                        totalRunningNum = taskNumArr[0];
                        baseRunningNum = taskNumArr[1];
                        cumuRunningNum = taskNumArr[2];
                        updateRunningNum = taskNumArr[3];
                        totalWaitingNum = taskNumArr[4];
                        baseWaitingNum = taskNumArr[5];
                        cumuWaitingNum = taskNumArr[6];
                    } else {
                        LOG.error("Failed to get backend: {} compaction task num info {}", backendId, res);
                    }
                } catch (IOException e) {
                    LOG.error("Failed to get backend: {} compaction task num info", backendId, e);
                }

                runningTaskUrl = String.format(
                        "http://%s:%d/api/compaction/running/show",
                        backend.getHost(),
                        backend.getHttpPort());
                waitingTaskUrl = String.format("http://%s:%d/api/compaction/waiting/show",
                        backend.getHost(),
                        backend.getHttpPort());

            }
            result.addRow(Arrays.asList(String.valueOf(backendId), totalRunningNum, baseRunningNum, cumuRunningNum,
                    updateRunningNum, totalWaitingNum, baseWaitingNum, cumuWaitingNum, runningTaskUrl, waitingTaskUrl));
        }

        return result;
    }
}

