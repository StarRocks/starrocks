// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

public class Constants {

    // PENDING -> RUNNING -> FAILED
    //                    -> SUCCESS
    public enum TaskRunState {
        PENDING,
        RUNNING,
        FAILED,
        SUCCESS,
    }

    public enum TaskSource {
        CTAS,
        MV
    }
}
