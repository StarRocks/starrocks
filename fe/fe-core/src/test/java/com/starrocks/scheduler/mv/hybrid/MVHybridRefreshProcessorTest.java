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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedView.RefreshMode;
import com.starrocks.catalog.MaterializedView.RefreshModeReason;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.MVRefreshParams;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MVHybridRefreshProcessorTest {

    private static MVRefreshParams rangePartitionedParams(Map<String, String> properties) {
        MaterializedView mv = Mockito.mock(MaterializedView.class);
        PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
        Mockito.when(mv.getPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(partitionInfo.isUnPartitioned()).thenReturn(false);
        Mockito.when(partitionInfo.isRangePartition()).thenReturn(true);
        Mockito.when(partitionInfo.isListPartition()).thenReturn(false);
        Mockito.when(mv.getPartitionRefreshStrategy())
                .thenReturn(MaterializedView.PartitionRefreshStrategy.STRICT);
        return new MVRefreshParams(mv, properties);
    }

    @Test
    public void aCompleteUnforcedRefreshMayTryIncremental() {
        assertFalse(MVHybridRefreshProcessor.isPctOnly(rangePartitionedParams(new HashMap<>())));
    }

    @Test
    public void aPartialRefreshIsPctOnly() {
        Map<String, String> properties = new HashMap<>();
        properties.put(TaskRun.PARTITION_START, "2026-01-01");
        assertTrue(MVHybridRefreshProcessor.isPctOnly(rangePartitionedParams(properties)));
    }

    @Test
    public void aForcedRefreshIsPctOnly() {
        Map<String, String> properties = new HashMap<>();
        properties.put(TaskRun.FORCE, "true");
        assertTrue(MVHybridRefreshProcessor.isPctOnly(rangePartitionedParams(properties)));
    }

    private static RuntimeException cdcError(String detail) {
        return new RuntimeException("CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): " + detail);
    }

    private static Optional<RefreshModeReason> reasonFor(Throwable e) {
        return MVHybridRefreshProcessor.fallbackReasonOnExecutionFailure(
                RefreshMode.INCREMENTAL, RefreshMode.AUTO, e);
    }

    @Test
    public void aDeleteRejectionIsANonAppendOnlyChange() {
        assertEquals(Optional.of(RefreshModeReason.NON_APPEND_ONLY_CHANGE),
                reasonFor(cdcError("CDC for DUP_KEYS does not support delete")));
        assertEquals(Optional.of(RefreshModeReason.NON_APPEND_ONLY_CHANGE),
                reasonFor(cdcError("CDC for AGG_KEYS does not support delete")));
        assertEquals(Optional.of(RefreshModeReason.NON_APPEND_ONLY_CHANGE),
                reasonFor(new RuntimeException("refresh failed", new IllegalStateException(
                        "query failed: CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): "
                                + "CDC for DUP_KEYS does not support delete"))));
    }

    @Test
    public void aCaptureDisabledRejectionNamesTheSetting() {
        assertEquals(Optional.of(RefreshModeReason.CHANGE_CAPTURE_DISABLED),
                reasonFor(cdcError("CHANGES window on tablet 42 spans version 3 which was not recorded "
                        + "(change data capture was not enabled at that version)")));
    }

    @Test
    public void everyOtherRejectionStaysUnknown() {
        assertEquals(Optional.of(RefreshModeReason.UNKNOWN),
                reasonFor(cdcError("CHANGES ancestor chain on tablet 42 cannot reach base version 7 "
                        + "from version 9")));
        assertEquals(Optional.of(RefreshModeReason.UNKNOWN),
                reasonFor(cdcError("CHANGES window on tablet 42 spans version 3 whose changes were not "
                        + "captured: degraded by recover")));
    }

    /**
     * An INCREMENTAL view reaches this processor too, whenever a base table needs its TVR baseline rebuilt.
     * Falling back there would hand it the approximate semantics it declined, so the settled mode -- not the
     * mode this run happens to be executing -- decides.
     */
    @Test
    public void anIncrementalViewNeverFallsBack() {
        assertTrue(MVHybridRefreshProcessor.fallbackReasonOnExecutionFailure(
                RefreshMode.INCREMENTAL, RefreshMode.INCREMENTAL,
                cdcError("CDC for DUP_KEYS does not support delete")).isEmpty());
    }

    @Test
    public void aRunAlreadyOnPctDoesNotRetry() {
        assertTrue(MVHybridRefreshProcessor.fallbackReasonOnExecutionFailure(
                RefreshMode.PCT, RefreshMode.AUTO,
                cdcError("CDC for DUP_KEYS does not support delete")).isEmpty());
    }

    @Test
    public void anyOtherFailureIsRethrown() {
        assertTrue(reasonFor(new RuntimeException("get database write lock timeout")).isEmpty());
        assertTrue(reasonFor(new RuntimeException(
                "INCREMENTAL materialized views do not support non-append-only base changes")).isEmpty());
    }
}
