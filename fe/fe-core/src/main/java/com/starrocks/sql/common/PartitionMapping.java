// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.common;
import java.time.LocalDateTime;

public class PartitionMapping {

    // the lowerDateTime is closed
    private final LocalDateTime lowerDateTime;
    // the upperDateTime is open
    private final LocalDateTime upperDateTime;

    public PartitionMapping(LocalDateTime lowerDateTime, LocalDateTime upperDateTime) {

        this.lowerDateTime = lowerDateTime;
        this.upperDateTime = upperDateTime;
    }


    public LocalDateTime getLowerDateTime() {
        return lowerDateTime;
    }

    public LocalDateTime getUpperDateTime() {
        return upperDateTime;
    }
}
