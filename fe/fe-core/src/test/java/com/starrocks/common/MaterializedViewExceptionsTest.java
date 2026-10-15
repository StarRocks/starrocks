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

<<<<<<< HEAD
import org.junit.jupiter.api.Test;

import java.util.Collections;
=======
import com.starrocks.scheduler.MVActiveChecker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.MVInactiveException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
>>>>>>> 103084cf4fc... [Refactor][RB-390] Classify MV refresh failures by type instead of message text (#65255)

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MaterializedViewExceptionsTest {

    @Test
<<<<<<< HEAD
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

    /**
     * A column ALTER on an IVM MV knocks its stored schema out of position-alignment with the
     * rewritten maintenance query; IvmSchemaCompat reports that through these two reasons. The
     * mismatch is deterministic -- it never heals -- so it must classify as breaking and inactivate
     * the MV, rather than leaving is_active true with only a FAILED run. Both the pure-IVM path and
     * the hybrid fallback-to-PCT path surface the same message.
     */
=======
    public void testForceRefreshRecoversOnlyABrokenOrUnreachableBaseline() {
        assertTrue(MaterializedViewExceptions.isRecoveredByForceRefresh(
                MaterializedViewExceptions.inactiveReasonForIncrementalBreaking("mv1")));
        assertTrue(MaterializedViewExceptions.isRecoveredByForceRefresh(
                MaterializedViewExceptions.INACTIVE_REASON_FOR_BASELINE_UNREACHABLE + "mv1"));
        assertFalse(MaterializedViewExceptions.isRecoveredByForceRefresh(
                MaterializedViewExceptions.INACTIVE_REASON_FOR_MV_SCHEMA_MISMATCH + "mv1"));
        assertFalse(MaterializedViewExceptions.isRecoveredByForceRefresh(
                MaterializedViewExceptions.inactiveReasonForConsecutiveFailures("mv1")));
        assertFalse(MaterializedViewExceptions.isRecoveredByForceRefresh(null));
    }

>>>>>>> 103084cf4fc... [Refactor][RB-390] Classify MV refresh failures by type instead of message text (#65255)
    @Test
    public void testPersistedReasonsAreStillRecognized() {
        assertTrue(MaterializedViewExceptions.isRecoveredByForceRefresh(
                "incremental refresh broken by non-append-only base change: mv1"));
        assertTrue(MaterializedViewExceptions.isRecoveredByForceRefresh(
                "incremental refresh broken: the base table state this view refreshes from is no longer available: mv1"));
        assertFalse(MaterializedViewExceptions.isRecoveredByForceRefresh(
                "incremental refresh broken: materialized view schema no longer matches its definition: mv1"));
        assertFalse(MaterializedViewExceptions.isRecoveredByForceRefresh("mv consecutive failures: mv1"));
        assertFalse(MaterializedViewExceptions.isRecoveredByForceRefresh(null));
        for (String reason : List.of(
                "incremental refresh broken by non-append-only base change: mv1",
                "incremental refresh broken: the base table state this view refreshes from is no longer available: mv1",
                "incremental refresh broken: materialized view schema no longer matches its definition: mv1",
                "mv consecutive failures: mv1")) {
            assertTrue(MVActiveChecker.isNonAutoActivatableReason(reason), reason);
        }
        assertFalse(MVActiveChecker.isNonAutoActivatableReason("base table schema changed for columns: v"));
    }

    @Test
<<<<<<< HEAD
    public void testBreakingFailureReasonFollowsTheMatchedCause() {
        assertTrue(MaterializedViewExceptions.inactiveReasonForBreakingFailure(
                        new RuntimeException(MaterializedViewExceptions.inactiveReasonForColumnChanged(
                                Collections.singleton("column count 6 vs 7"))), "mv1")
                .startsWith(MaterializedViewExceptions.INACTIVE_REASON_FOR_MV_SCHEMA_MISMATCH));
        // a non-append-only breakage keeps its own reason rather than being relabelled a schema mismatch
        assertTrue(MaterializedViewExceptions.inactiveReasonForBreakingFailure(
                        new RuntimeException("x " + MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER + " y"), "mv1")
                .startsWith(MaterializedViewExceptions.INACTIVE_REASON_FOR_INCREMENTAL_BREAKING));
=======
    public void testKindOfFindsTheClassifiedFailureAlongTheCauseChain() {
        MVInactiveException inactive = new MVInactiveException(
                "Materialized view: mv1/1 is not active due to base table schema changed for columns: v.");
        assertEquals(Optional.of(RefreshFailureKind.VIEW_INACTIVE), MaterializedViewExceptions.kindOf(inactive));
        assertEquals(Optional.of(RefreshFailureKind.VIEW_INACTIVE), MaterializedViewExceptions.kindOf(
                new DmlException("Refresh mv mv1 failed after 1 times, error-msg : " + inactive, inactive)));
        assertEquals(Optional.of(RefreshFailureKind.VIEW_INACTIVE), MaterializedViewExceptions.kindOf(
                new SemanticException("wrapped", inactive)));
    }

    @Test
    public void testKindOfIgnoresSuppressedFailuresAndMessageText() {
        RuntimeException pctFailure = new RuntimeException("pct failed");
        pctFailure.addSuppressed(new MVInactiveException("is not active"));
        assertEquals(Optional.empty(), MaterializedViewExceptions.kindOf(pctFailure));
        assertEquals(Optional.empty(), MaterializedViewExceptions.kindOf(new RuntimeException(
                "x " + MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER + " "
                        + MaterializedViewExceptions.MV_SCHEMA_COLUMN_CHANGED_MARKER + "v")));
        assertEquals(Optional.empty(), MaterializedViewExceptions.kindOf(null));
    }

    @Test
    public void testOnlyBreakingKindsHaveAnInactiveReason() {
        assertFalse(RefreshFailureKind.VIEW_INACTIVE.isBreaking());
        assertThrows(IllegalStateException.class, () -> RefreshFailureKind.VIEW_INACTIVE.inactiveReason("mv1"));
        assertEquals("incremental refresh broken by non-append-only base change: mv1",
                RefreshFailureKind.INCREMENTAL_BROKEN.inactiveReason("mv1"));
        assertEquals("incremental refresh broken: the base table state this view refreshes from is no longer "
                + "available: mv1", RefreshFailureKind.BASELINE_UNREACHABLE.inactiveReason("mv1"));
        assertEquals("incremental refresh broken: materialized view schema no longer matches its definition: mv1",
                RefreshFailureKind.SCHEMA_MISMATCH.inactiveReason("mv1"));
        assertTrue(RefreshFailureKind.INCREMENTAL_BROKEN.isRecoveredByForce());
        assertTrue(RefreshFailureKind.BASELINE_UNREACHABLE.isRecoveredByForce());
        assertFalse(RefreshFailureKind.SCHEMA_MISMATCH.isRecoveredByForce());
>>>>>>> 103084cf4fc... [Refactor][RB-390] Classify MV refresh failures by type instead of message text (#65255)
    }
}
