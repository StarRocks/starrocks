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
package com.starrocks.common;

import com.google.common.base.Preconditions;

public enum RefreshFailureKind {
    VIEW_INACTIVE(null, false),
    INCREMENTAL_BROKEN(MaterializedViewExceptions.INACTIVE_REASON_FOR_INCREMENTAL_BREAKING, true),
    BASELINE_UNREACHABLE(MaterializedViewExceptions.INACTIVE_REASON_FOR_BASELINE_UNREACHABLE, true),
    SCHEMA_MISMATCH(MaterializedViewExceptions.INACTIVE_REASON_FOR_MV_SCHEMA_MISMATCH, false);

    private final String inactiveReasonPrefix;
    private final boolean recoveredByForce;

    RefreshFailureKind(String inactiveReasonPrefix, boolean recoveredByForce) {
        this.inactiveReasonPrefix = inactiveReasonPrefix;
        this.recoveredByForce = recoveredByForce;
    }

    public boolean isBreaking() {
        return inactiveReasonPrefix != null;
    }

    public boolean isRecoveredByForce() {
        return recoveredByForce;
    }

    /** Null for a kind that does not break the view. */
    public String inactiveReasonPrefix() {
        return inactiveReasonPrefix;
    }

    public String inactiveReason(String mvName) {
        Preconditions.checkState(isBreaking(), "%s does not inactivate a view", this);
        return inactiveReasonPrefix + mvName;
    }
}
