// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.load.routineload;

import com.starrocks.common.LoadException;

/**
 * Routine load job should be paused when catch this exception
 */
public class RoutineLoadPauseException extends LoadException {
    public RoutineLoadPauseException(String msg) {
        super(msg);
    }

    public RoutineLoadPauseException(String msg, Throwable e) {
        super(msg, e);
    }
}
