// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.RefreshType;

import java.time.LocalDateTime;

public class AsyncRefreshSchemeDesc extends RefreshSchemeDesc {

    private LocalDateTime startTime;

    private long step;

    private TimestampArithmeticExpr.TimeUnit timeUnit;

    private final ImmutableSet<TimestampArithmeticExpr.TimeUnit> supportedTimeUnitType = ImmutableSet.of(
            TimestampArithmeticExpr.TimeUnit.MINUTE,
            TimestampArithmeticExpr.TimeUnit.HOUR,
            TimestampArithmeticExpr.TimeUnit.DAY,
            TimestampArithmeticExpr.TimeUnit.WEEK,
            TimestampArithmeticExpr.TimeUnit.MONTH,
            TimestampArithmeticExpr.TimeUnit.YEAR
    );

    public AsyncRefreshSchemeDesc(LocalDateTime startTime, long step, TimestampArithmeticExpr.TimeUnit timeUnit) {
        super(RefreshType.ASYNC);
        this.startTime = startTime;
        this.step = step;
        this.timeUnit = timeUnit;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }


    public ImmutableSet<TimestampArithmeticExpr.TimeUnit> getSupportedTimeUnitType() {
        return supportedTimeUnitType;
    }

    public long getStep() {
        return step;
    }

    public void setStep(long step) {
        this.step = step;
    }

    public TimestampArithmeticExpr.TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(TimestampArithmeticExpr.TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }
}

