// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.journal;

public class JournalWriteException extends Exception {
    public enum Reason {
        // The caller tried to submit leader-only journal work on a node that is not leader.
        NOT_LEADER,
        // Local leader demotion has started, so new leader-only journal work must be rejected
        // even if the FE type has not fully switched yet.
        ADMISSION_CLOSED,
        // The task was accepted before, but the writer aborted it while sealing or closing.
        WRITER_ABORTED,
        // Waiting for a journal task exceeded the caller-provided timeout.
        TIMEOUT
    }

    private final Reason reason;

    public JournalWriteException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public JournalWriteException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }
}
