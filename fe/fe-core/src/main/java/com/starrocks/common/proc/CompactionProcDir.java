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

import java.util.Arrays;

public class CompactionProcDir implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES;

    static {
        ImmutableList.Builder<String> builder = new ImmutableList.Builder<String>()
                .add("BackendId").add("RunningCompactionTasks").add("WaitingCompactionTasks");
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
            String runningTaskUrl;
            String waitingTaskUrl;
            if (backendId != null) {
                Backend backend = clusterInfoService.getBackend(backendId);
                runningTaskUrl = String.format(
                        "http://%s:%d/api/compaction/running/show",
                        backend.getHost(),
                        backend.getHttpPort());
                waitingTaskUrl = String.format("http://%s:%d/api/compaction/waiting/show",
                        backend.getHost(),
                        backend.getHttpPort());
            } else {
                runningTaskUrl = "N/A";
                waitingTaskUrl = "N/A";
            }
            result.addRow(Arrays.asList(String.valueOf(backendId), runningTaskUrl, waitingTaskUrl));
        }

        return result;
    }
}
