package com.starrocks.load;

public enum InsertOverwriteJobState {
    OVERWRITE_PENDING,
    OVERWRITE_PREPARED,
    OVERWRITE_SUCCESS,
    OVERWRITE_FAILED,
    OVERWRITE_CANCELLED
}
