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

import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.Coordinator;

public class LoadErrorUtils {

    public static class ErrorMeta {
        final String keywords;
        final String description;

        public ErrorMeta(String keywords, String description) {
            this.keywords = keywords;
            this.description = description;
        }

        public String getKeywords() {
            return keywords;
        }

        public String getDescription() {
            return description;
        }
    }

    public static final ErrorMeta BACKEND_BRPC_TIMEOUT =
            new ErrorMeta("[E1008]Reached timeout", "Backend BRPC timeout");

    private static final ErrorMeta[] LOADING_TASK_TIMEOUT_ERRORS = new ErrorMeta[] {BACKEND_BRPC_TIMEOUT};

    public static boolean isTimeoutFromLoadingTaskExecution(String errorMsg) {
        for (ErrorMeta errorMeta : LOADING_TASK_TIMEOUT_ERRORS) {
            if (errorMsg.contains(errorMeta.keywords)) {
                return true;
            }
        }
        return false;
    }

    public static boolean enableProfileAfterError(Coordinator coordinator) {
        if (!(coordinator instanceof DefaultCoordinator defaultCoordinator)) {
            return false;
        }
        if (defaultCoordinator.getExecStatus() == null || defaultCoordinator.getExecStatus().ok()
                || defaultCoordinator.getExecStatus().getErrorMsg() == null) {
            return false;
        }
        if (!defaultCoordinator.getQueryRuntimeProfile().hasLoadChannelProfile()) {
            return false;
        }
        return defaultCoordinator.getExecStatus().getErrorMsg().contains(BACKEND_BRPC_TIMEOUT.getKeywords());
    }
}
