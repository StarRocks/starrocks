// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.catalog.RefreshType;

import java.time.LocalDateTime;

public class AsyncRefreshSchemeDesc extends RefreshSchemeDesc {

    private LocalDateTime startTime;

    private IntervalLiteral intervalLiteral;

    public AsyncRefreshSchemeDesc(LocalDateTime startTime, IntervalLiteral intervalLiteral) {
        super(RefreshType.ASYNC);
        this.startTime = startTime;
        this.intervalLiteral = intervalLiteral;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public IntervalLiteral getIntervalLiteral() {
        return intervalLiteral;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }
}

