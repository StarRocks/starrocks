// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.common;

import com.google.common.collect.Range;
import com.starrocks.catalog.PartitionKey;

import java.time.LocalDateTime;

public class PartitionMapping {

    private final LocalDateTime firstTime;
    private final LocalDateTime lastTime;

    public PartitionMapping(LocalDateTime firstTime, LocalDateTime lastTime) {

        this.firstTime = firstTime;
        this.lastTime = lastTime;
    }


    public LocalDateTime getFirstTime() {
        return firstTime;
    }

    public LocalDateTime getLastTime() {
        return lastTime;
    }
}
