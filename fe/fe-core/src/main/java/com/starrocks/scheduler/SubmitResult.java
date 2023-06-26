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

package com.starrocks.scheduler;

import java.util.concurrent.Future;

public class SubmitResult {
    private String queryId;
    private SubmitStatus status;
    private Future<Constants.TaskRunState> future;

    public SubmitResult(String queryId, SubmitStatus status) {
        this.queryId = queryId;
        this.status = status;
    }

    public SubmitResult(String queryId, SubmitStatus status, Future<Constants.TaskRunState> future) {
        this.queryId = queryId;
        this.status = status;
        this.future = future;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public SubmitStatus getStatus() {
        return status;
    }

    public void setStatus(SubmitStatus status) {
        this.status = status;
    }

    public Future<Constants.TaskRunState> getFuture() {
        return future;
    }

    public void setFuture(Future<Constants.TaskRunState> future) {
        this.future = future;
    }

    public enum SubmitStatus {
        SUBMITTED,
        REJECTED,
        FAILED
    }
}
