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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MaterializedViewExceptionsTest {

    @Test
    public void testIsIncrementalBreakingFailure() {
        // FE-detected non-append-only breakage; built from the shared marker so the test can't drift from detection.
        assertTrue(MaterializedViewExceptions.isIncrementalBreakingFailure(new RuntimeException(
                "Cannot incrementally refresh materialized view mv1: non-append-only change on base table db.t. "
                        + "INCREMENTAL materialized views " + MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER
                        + " (DELETE / OVERWRITE / DROP PARTITION / snapshot expiration / table replacement).")));
        // the marker may be wrapped deeper in the cause chain
        assertTrue(MaterializedViewExceptions.isIncrementalBreakingFailure(
                new RuntimeException("refresh failed",
                        new IllegalStateException("... " + MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER + " ..."))));

        // transient / unrelated failures must not be treated as breaking
        assertFalse(MaterializedViewExceptions.isIncrementalBreakingFailure(
                new RuntimeException("get database write lock timeout")));
        assertFalse(MaterializedViewExceptions.isIncrementalBreakingFailure(
                new RuntimeException("No checkpoint found for base table: db.t during IVM planning")));
        assertFalse(MaterializedViewExceptions.isIncrementalBreakingFailure(new RuntimeException()));
        assertFalse(MaterializedViewExceptions.isIncrementalBreakingFailure(null));
    }
}
