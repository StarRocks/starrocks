// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.catalog.RefreshType;
import com.starrocks.common.UserException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.RefreshSchemeDesc;

import java.time.LocalDateTime;

public class AsyncRefreshSchemeDesc extends RefreshSchemeDesc {

    private LocalDateTime startTime;

    private IntervalLiteral intervalLiteral;

    public AsyncRefreshSchemeDesc(LocalDateTime startTime, IntervalLiteral intervalLiteral) {
        this.type = RefreshType.ASYNC;
        this.startTime = startTime;
        this.intervalLiteral = intervalLiteral;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public IntervalLiteral getIntervalLiteral() {
        return intervalLiteral;
    }
}

