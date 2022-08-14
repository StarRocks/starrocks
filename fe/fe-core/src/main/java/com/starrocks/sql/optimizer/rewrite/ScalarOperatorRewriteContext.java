// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite;

public class ScalarOperatorRewriteContext {
    // mark operator rewrite nums
    private int changeNum;

    public void reset() {
        changeNum = 0;
    }

    public void change() {
        changeNum++;
    }

    public boolean hasChanged() {
        return changeNum > 0;
    }

    public int changeNum() {
        return changeNum;
    }
}
