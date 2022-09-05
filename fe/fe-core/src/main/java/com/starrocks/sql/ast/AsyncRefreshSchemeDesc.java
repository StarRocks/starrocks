// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.catalog.MaterializedView;

import java.time.LocalDateTime;

public class AsyncRefreshSchemeDesc extends RefreshSchemeDesc {

    private boolean defineStartTime;

    private LocalDateTime startTime;

    private IntervalLiteral intervalLiteral;

    public AsyncRefreshSchemeDesc(boolean defineStartTime, LocalDateTime startTime, IntervalLiteral intervalLiteral) {
        super(MaterializedView.RefreshType.ASYNC);
        this.defineStartTime = defineStartTime;
        this.startTime = startTime;
        this.intervalLiteral = intervalLiteral;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public IntervalLiteral getIntervalLiteral() {
        return intervalLiteral;
    }

    public boolean isDefineStartTime() {
        return defineStartTime;
    }
}

