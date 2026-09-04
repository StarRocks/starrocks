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

import com.starrocks.thrift.TCdcErrorCode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CdcErrorUtilsTest {
    @Test
    public void testFindValidEnvelope() {
        CdcErrorUtils.Parsed parsed = CdcErrorUtils.find(
                "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): tablet 42 history is unavailable").orElseThrow();
        assertEquals(TCdcErrorCode.CHANGE_NOT_TRACKABLE, parsed.getCode());
        assertEquals("tablet 42 history is unavailable", parsed.getMessage());

        assertEquals(TCdcErrorCode.CHANGE_NOT_TRACKABLE,
                CdcErrorUtils.find("insert failed: CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): wrapped detail")
                        .orElseThrow().getCode());

        assertEquals("first line\nsecond line",
                CdcErrorUtils.find("wrapper: CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): first line\nsecond line")
                        .orElseThrow().getMessage());

        String headerShapedDetail = "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): nested diagnostic";
        assertEquals(headerShapedDetail,
                CdcErrorUtils.find("CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): " + headerShapedDetail)
                        .orElseThrow().getMessage());
    }

    @Test
    public void testFindValidEnvelopeAfterRejectedCandidate() {
        CdcErrorUtils.Parsed afterUnknown = CdcErrorUtils.find(
                "CDC-ERROR-99 (CHANGE_NOT_TRACKABLE): unknown code\n"
                        + "query failed: CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): valid detail after unknown")
                .orElseThrow();
        assertEquals(TCdcErrorCode.CHANGE_NOT_TRACKABLE, afterUnknown.getCode());
        assertEquals("valid detail after unknown", afterUnknown.getMessage());

        CdcErrorUtils.Parsed afterMismatch = CdcErrorUtils.find(
                "CDC-ERROR-1 (CHANGES_NOT_TRACKABLE): mismatched symbol\n"
                        + "caused by: CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): valid detail after mismatch")
                .orElseThrow();
        assertEquals(TCdcErrorCode.CHANGE_NOT_TRACKABLE, afterMismatch.getCode());
        assertEquals("valid detail after mismatch", afterMismatch.getMessage());
    }

    @Test
    public void testFindEmptyMessage() {
        CdcErrorUtils.Parsed parsed = CdcErrorUtils.find(
                "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): ").orElseThrow();
        assertEquals(TCdcErrorCode.CHANGE_NOT_TRACKABLE, parsed.getCode());
        assertEquals("", parsed.getMessage());
    }

    @Test
    public void testIsChangeNotTrackable() {
        assertTrue(CdcErrorUtils.isChangeNotTrackable(
                "query failed: CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): history missing"));
        assertTrue(CdcErrorUtils.isChangeNotTrackable("CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): "));
        assertFalse(CdcErrorUtils.isChangeNotTrackable("CDC-ERROR-0 (UNKNOWN): detail"));
        assertFalse(CdcErrorUtils.isChangeNotTrackable("CDC-ERROR-1 (CHANGES_NOT_TRACKABLE): detail"));
        assertFalse(CdcErrorUtils.isChangeNotTrackable(null));
    }

    /** The literals are copied from changes_connector.cpp; a reword there must fail here. */
    @Test
    public void testIsRowDeleteRejection() {
        assertTrue(CdcErrorUtils.isRowDeleteRejection(
                "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for DUP_KEYS does not support delete"));
        assertTrue(CdcErrorUtils.isRowDeleteRejection(
                "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for AGG_KEYS does not support delete"));

        assertFalse(CdcErrorUtils.isRowDeleteRejection("CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CHANGES ancestor "
                + "chain on tablet 42 cannot reach base version 7 from version 9"));
        assertFalse(CdcErrorUtils.isRowDeleteRejection("CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CHANGES window on "
                + "tablet 42 spans version 3 which was not recorded (change data capture was not enabled at "
                + "that version)"));
        assertFalse(CdcErrorUtils.isRowDeleteRejection("CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CHANGES window on "
                + "tablet 42 spans version 3 whose changes were not captured: degraded by recover"));

        // The phrase outside a CDC envelope is not the backend's rejection.
        assertFalse(CdcErrorUtils.isRowDeleteRejection("this table does not support delete"));
        assertFalse(CdcErrorUtils.isRowDeleteRejection(null));
    }

    /** Same contract as isRowDeleteRejection: the literal comes from changes_connector.cpp. */
    @Test
    public void testIsCaptureDisabledRejection() {
        assertTrue(CdcErrorUtils.isCaptureDisabledRejection("CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CHANGES "
                + "window on tablet 42 spans version 3 which was not recorded (change data capture was not "
                + "enabled at that version)"));

        assertFalse(CdcErrorUtils.isCaptureDisabledRejection("CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CHANGES "
                + "window on tablet 42 spans version 3 whose changes were not captured: degraded by recover"));
        assertFalse(CdcErrorUtils.isCaptureDisabledRejection(
                "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for DUP_KEYS does not support delete"));
        assertFalse(CdcErrorUtils.isCaptureDisabledRejection(null));
    }

    @Test
    public void testRejectMalformedOrInconsistentEnvelope() {
        List<String> invalid = List.of(
                "CDC-ERROR-0 (UNKNOWN): detail",
                "CDC-ERROR-99 (CHANGE_NOT_TRACKABLE): detail",
                "CDC-ERROR-1 (CHANGES_NOT_TRACKABLE): detail",
                "CDC-ERROR-1(CHANGE_NOT_TRACKABLE): detail",
                "CDC-ERROR-1 CHANGE_NOT_TRACKABLE: detail",
                "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE) detail",
                "DELETE_PREDICATE_FOUND: CHANGES not supported for DELETE operations");
        invalid.forEach(message -> assertFalse(CdcErrorUtils.find(message).isPresent(), message));
        assertFalse(CdcErrorUtils.find(null).isPresent());
    }
}
