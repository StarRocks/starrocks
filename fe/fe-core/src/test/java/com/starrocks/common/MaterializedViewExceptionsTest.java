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

import java.util.Collections;

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
        // BE-detected delete on a cloud-native CHANGES scan (OLAP row-level delete path)
        assertTrue(MaterializedViewExceptions.isIncrementalBreakingFailure(new RuntimeException(
                "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for DUP_KEYS does not support delete")));
        // either condition may be wrapped deeper in the cause chain
        assertTrue(MaterializedViewExceptions.isIncrementalBreakingFailure(
                new RuntimeException("refresh failed",
                        new IllegalStateException("... " + MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER + " ..."))));
        assertTrue(MaterializedViewExceptions.isIncrementalBreakingFailure(
                new RuntimeException("refresh failed",
                        new IllegalStateException(
                                "query failed: CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): history missing"))));

        // transient / unrelated failures must not be treated as breaking
        assertFalse(MaterializedViewExceptions.isIncrementalBreakingFailure(
                new RuntimeException("DELETE_PREDICATE_FOUND: CHANGES not supported for DELETE operations")));
        assertFalse(MaterializedViewExceptions.isIncrementalBreakingFailure(
                new RuntimeException("CDC-ERROR-2 (CHANGE_NOT_TRACKABLE): unknown code")));
        assertFalse(MaterializedViewExceptions.isIncrementalBreakingFailure(
                new RuntimeException("CDC-ERROR-1 (CHANGES_NOT_TRACKABLE): mismatched symbol")));
        assertFalse(MaterializedViewExceptions.isIncrementalBreakingFailure(
                new RuntimeException("Not supported: generic CHANGES failure without a CDC envelope")));
        assertFalse(MaterializedViewExceptions.isIncrementalBreakingFailure(
                new RuntimeException("get database write lock timeout")));
        assertFalse(MaterializedViewExceptions.isIncrementalBreakingFailure(
                new RuntimeException("No checkpoint found for base table: db.t during IVM planning")));
        assertFalse(MaterializedViewExceptions.isIncrementalBreakingFailure(new RuntimeException()));
        assertFalse(MaterializedViewExceptions.isIncrementalBreakingFailure(null));
    }

    @Test
    public void testIsChangeNotTrackableFailure() {
        assertTrue(MaterializedViewExceptions.isChangeNotTrackableFailure(new RuntimeException(
                "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for DUP_KEYS does not support delete")));
        assertTrue(MaterializedViewExceptions.isChangeNotTrackableFailure(new RuntimeException(
                "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for AGG_KEYS does not support delete")));
        assertTrue(MaterializedViewExceptions.isChangeNotTrackableFailure(
                new RuntimeException("refresh failed",
                        new IllegalStateException(
                                "query failed: CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): history missing"))));

        // The deliberate difference from isIncrementalBreakingFailure.
        assertFalse(MaterializedViewExceptions.isChangeNotTrackableFailure(new RuntimeException(
                "INCREMENTAL materialized views " + MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER)));

        assertFalse(MaterializedViewExceptions.isChangeNotTrackableFailure(
                new RuntimeException("CDC-ERROR-2 (CHANGE_NOT_TRACKABLE): unknown code")));
        assertFalse(MaterializedViewExceptions.isChangeNotTrackableFailure(
                new RuntimeException("get database write lock timeout")));
        assertFalse(MaterializedViewExceptions.isChangeNotTrackableFailure(new RuntimeException()));
        assertFalse(MaterializedViewExceptions.isChangeNotTrackableFailure(null));
    }

    /**
     * A column ALTER on an IVM MV knocks its stored schema out of position-alignment with the
     * rewritten maintenance query; IvmSchemaCompat reports that through these two reasons. The
     * mismatch is deterministic -- it never heals -- so it must classify as breaking and inactivate
     * the MV, rather than leaving is_active true with only a FAILED run. Both the pure-IVM path and
     * the hybrid fallback-to-PCT path surface the same message.
     */
    @Test
    public void testSchemaMismatchIsBreaking() {
        assertTrue(MaterializedViewExceptions.isIncrementalBreakingFailure(new RuntimeException(
                "Getting analyzing error. Detail message: "
                        + MaterializedViewExceptions.inactiveReasonForColumnChanged(
                                Collections.singleton("column count 6 vs 7")))));
        assertTrue(MaterializedViewExceptions.isIncrementalBreakingFailure(new RuntimeException(
                MaterializedViewExceptions.inactiveReasonForColumnNotCompatible(
                        "`imp` bigint", "`imp` largeint"))));
        assertTrue(MaterializedViewExceptions.isIncrementalBreakingFailure(
                new RuntimeException("Refresh mv s1_mv failed after 1 times",
                        new IllegalStateException(MaterializedViewExceptions.inactiveReasonForColumnChanged(
                                Collections.singleton("column count 6 vs 7"))))));
    }

    @Test
    public void testBreakingFailureReasonFollowsTheMatchedCause() {
        assertTrue(MaterializedViewExceptions.inactiveReasonForBreakingFailure(
                        new RuntimeException(MaterializedViewExceptions.inactiveReasonForColumnChanged(
                                Collections.singleton("column count 6 vs 7"))), "mv1")
                .startsWith(MaterializedViewExceptions.INACTIVE_REASON_FOR_MV_SCHEMA_MISMATCH));
        // a non-append-only breakage keeps its own reason rather than being relabelled a schema mismatch
        assertTrue(MaterializedViewExceptions.inactiveReasonForBreakingFailure(
                        new RuntimeException("x " + MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER + " y"), "mv1")
                .startsWith(MaterializedViewExceptions.INACTIVE_REASON_FOR_INCREMENTAL_BREAKING));
    }
}
