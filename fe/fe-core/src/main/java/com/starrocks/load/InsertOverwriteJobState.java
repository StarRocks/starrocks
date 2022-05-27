// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

public enum InsertOverwriteJobState {
    OVERWRITE_PENDING,
    OVERWRITE_PREPARED,
    OVERWRITE_SUCCESS,
    OVERWRITE_FAILED,
    OVERWRITE_CANCELLED
}
