// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.load;

public enum InsertOverwriteJobState {
    OVERWRITE_PENDING,
    OVERWRITE_RUNNING,
    OVERWRITE_SUCCESS,
    OVERWRITE_FAILED
}
