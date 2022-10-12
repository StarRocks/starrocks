// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

public class SubmitResult {
    String queryId;
    SubmitStatus status;

    public SubmitResult(String queryId, SubmitStatus status) {
        this.queryId = queryId;
        this.status = status;
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

    enum SubmitStatus {
        SUBMITTED,
        REJECTED,
        FAILED
    }
}
