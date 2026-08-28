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

package com.starrocks.scheduler.mv.hybrid;

import com.starrocks.catalog.MaterializedView.RefreshMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MVHybridRefreshProcessorTest {

    private static RuntimeException notTrackable() {
        return new RuntimeException(
                "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for DUP_KEYS does not support delete");
    }

    @Test
    public void anAutoViewFallsBackWhenTheBackendRejectsTheChangesScan() {
        assertTrue(MVHybridRefreshProcessor.canFallBackOnExecutionFailure(
                RefreshMode.INCREMENTAL, RefreshMode.AUTO, notTrackable()));
    }

    /**
     * An INCREMENTAL view reaches this processor too, whenever a base table needs its TVR baseline rebuilt.
     * Falling back there would hand it the approximate semantics it declined, so the settled mode -- not the
     * mode this run happens to be executing -- decides.
     */
    @Test
    public void anIncrementalViewNeverFallsBack() {
        assertFalse(MVHybridRefreshProcessor.canFallBackOnExecutionFailure(
                RefreshMode.INCREMENTAL, RefreshMode.INCREMENTAL, notTrackable()));
    }

    @Test
    public void aRunAlreadyOnPctDoesNotRetry() {
        assertFalse(MVHybridRefreshProcessor.canFallBackOnExecutionFailure(
                RefreshMode.PCT, RefreshMode.AUTO, notTrackable()));
    }

    @Test
    public void anyOtherFailureIsRethrown() {
        assertFalse(MVHybridRefreshProcessor.canFallBackOnExecutionFailure(
                RefreshMode.INCREMENTAL, RefreshMode.AUTO,
                new RuntimeException("get database write lock timeout")));
        assertFalse(MVHybridRefreshProcessor.canFallBackOnExecutionFailure(
                RefreshMode.INCREMENTAL, RefreshMode.AUTO,
                new RuntimeException("INCREMENTAL materialized views do not support non-append-only base changes")));
    }
}
