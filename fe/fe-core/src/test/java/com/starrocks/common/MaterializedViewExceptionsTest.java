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
=======
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.scheduler.MVActiveChecker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.mv.IvmBrokenException;
>>>>>>> fa899f92593... [Enhancement][RB-754] Make materialized view refresh failures say what failed and how to recover (#66969)
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.MVInactiveException;
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
    public void testARefreshOfAnInactiveViewIsNotBreaking() {
        MVInactiveException inactive = new MVInactiveException(
                "Materialized view: mv1/1 is not active due to base table schema changed for columns: v.");
        assertFalse(MaterializedViewExceptions.isIncrementalBreakingFailure(inactive));
        // the refresh retry wraps it in an exception whose message quotes it, marker included
        assertFalse(MaterializedViewExceptions.isIncrementalBreakingFailure(
                new DmlException("Refresh mv mv1 failed after 1 times, error-msg : " + inactive, inactive)));
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
    public void testRefreshFailureMessageIsTheClassifiedMessageElseTheRootCauseWithItsTopFrames() {
        String detail = "Cannot incrementally refresh materialized view mv1: base table t1 was dropped and re-created. "
                + "Run ALTER MATERIALIZED VIEW `db1`.`mv1` ACTIVE to recover.";
        IvmBrokenException broken = new IvmBrokenException(RefreshFailureKind.BASELINE_UNREACHABLE, detail,
                new IllegalStateException("connector detail"));
        assertEquals(detail, MaterializedViewExceptions.refreshFailureMessage(
                new DmlException("Refresh mv mv1 failed after 1 times", broken), "q1"));

        IllegalStateException rootCause = new IllegalStateException("starlet err: object does not exist");
        DmlException unclassified = new DmlException("Refresh mv mv1 failed after 1 times",
                new RuntimeException("wrapped", rootCause));
        String message = MaterializedViewExceptions.refreshFailureMessage(unclassified, "q1");
        assertTrue(rootCause.getStackTrace().length > 5);
        StringBuilder expected = new StringBuilder("IllegalStateException: starlet err: object does not exist");
        for (int i = 0; i < 5; i++) {
            expected.append("\n\tat ").append(rootCause.getStackTrace()[i]);
        }
        expected.append("\n\t... full stack trace in fe.log, query_id: q1");
        assertEquals(expected.toString(), message);
        assertEquals("", MaterializedViewExceptions.refreshFailureMessage(null, "q1"));
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
        assertTrue(RefreshFailureKind.INCREMENTAL_BROKEN.invalidatesBaseline());
        assertTrue(RefreshFailureKind.BASELINE_UNREACHABLE.invalidatesBaseline());
        assertFalse(RefreshFailureKind.SCHEMA_MISMATCH.invalidatesBaseline());
>>>>>>> fa899f92593... [Enhancement][RB-754] Make materialized view refresh failures say what failed and how to recover (#66969)
    }
}
