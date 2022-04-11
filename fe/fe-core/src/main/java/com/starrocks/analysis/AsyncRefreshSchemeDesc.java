// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.common.UserException;
import com.starrocks.sql.ast.AstVisitor;

public class AsyncRefreshSchemeDesc extends RefreshSchemeDesc {

    private long startTime;

    private long step;

    private String timeUnit;

    public AsyncRefreshSchemeDesc(long startTime, long step, String timeUnit) {
        this.startTime = startTime;
        this.step = step;
        this.timeUnit = timeUnit;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getStep() {
        return step;
    }

    public String getTimeUnit() {
        return timeUnit;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return super.accept(visitor, context);
    }
}

