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

package com.starrocks.paimon.reader;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Base64;
import java.util.List;

public class PaimonRuntimePredicatesTest {

    private static final char FIELD_SEP = '\u0001';
    private static final char RECORD_SEP = '\u0003';

    private static final RowType ROW_TYPE = RowType.builder()
            .field("project_id", DataTypes.BIGINT())
            .field("email", DataTypes.STRING())
            .field("country", DataTypes.STRING())
            .field("ts", DataTypes.TIMESTAMP())
            .field("birthday", DataTypes.DATE())
            .build();

    private static String b64(String v) {
        return Base64.getEncoder().encodeToString(v.getBytes(StandardCharsets.UTF_8));
    }

    private static String record(String col, String op, String... values) {
        StringBuilder sb = new StringBuilder();
        sb.append(col).append(FIELD_SEP).append(op);
        for (String v : values) {
            sb.append(FIELD_SEP).append(b64(v));
        }
        return sb.toString();
    }

    private static GenericRow row(Long projectId, String email) {
        return GenericRow.of(projectId, email == null ? null : BinaryString.fromString(email), null, null, null);
    }

    @Test
    public void testMinMaxBigint() {
        String encoded = record("project_id", "minmax", "1", "1");
        List<Predicate> predicates = PaimonRuntimePredicates.parse(ROW_TYPE, encoded);
        Assertions.assertEquals(1, predicates.size());
        Predicate p = predicates.get(0);
        Assertions.assertTrue(p.test(row(1L, "a@b.com")));
        Assertions.assertFalse(p.test(row(2L, "a@b.com")));
        Assertions.assertFalse(p.test(row(null, "a@b.com")));
    }

    @Test
    public void testMinMaxString() {
        String encoded = record("email", "minmax", "a@b.com", "c@d.com");
        List<Predicate> predicates = PaimonRuntimePredicates.parse(ROW_TYPE, encoded);
        Assertions.assertEquals(1, predicates.size());
        Predicate p = predicates.get(0);
        Assertions.assertTrue(p.test(row(1L, "a@b.com")));
        Assertions.assertTrue(p.test(row(1L, "b@x.com")));
        Assertions.assertTrue(p.test(row(1L, "c@d.com")));
        Assertions.assertFalse(p.test(row(1L, "d@e.com")));
        Assertions.assertFalse(p.test(row(1L, null)));
    }

    @Test
    public void testInString() {
        String encoded = record("email", "in", "a@b.com", "x@y.com");
        List<Predicate> predicates = PaimonRuntimePredicates.parse(ROW_TYPE, encoded);
        Assertions.assertEquals(1, predicates.size());
        Predicate p = predicates.get(0);
        Assertions.assertTrue(p.test(row(1L, "a@b.com")));
        Assertions.assertTrue(p.test(row(1L, "x@y.com")));
        Assertions.assertFalse(p.test(row(1L, "b@c.com")));
        Assertions.assertFalse(p.test(row(1L, null)));
    }

    @Test
    public void testInBigint() {
        String encoded = record("project_id", "in", "1", "3");
        List<Predicate> predicates = PaimonRuntimePredicates.parse(ROW_TYPE, encoded);
        Assertions.assertEquals(1, predicates.size());
        Predicate p = predicates.get(0);
        Assertions.assertTrue(p.test(row(1L, "a")));
        Assertions.assertTrue(p.test(row(3L, "a")));
        Assertions.assertFalse(p.test(row(2L, "a")));
    }

    @Test
    public void testMultipleRecordsAreAnded() {
        String encoded = record("project_id", "minmax", "1", "1")
                + RECORD_SEP
                + record("email", "in", "a@b.com");
        List<Predicate> predicates = PaimonRuntimePredicates.parse(ROW_TYPE, encoded);
        Assertions.assertEquals(2, predicates.size());
    }

    @Test
    public void testDateMinMax() {
        String lo = LocalDate.of(2024, 1, 1).toString();
        String hi = LocalDate.of(2024, 12, 31).toString();
        String encoded = record("birthday", "minmax", lo, hi);
        List<Predicate> predicates = PaimonRuntimePredicates.parse(ROW_TYPE, encoded);
        Assertions.assertEquals(1, predicates.size());
        GenericRow inside = GenericRow.of(null, null, null, null, (int) LocalDate.of(2024, 6, 1).toEpochDay());
        GenericRow outside = GenericRow.of(null, null, null, null, (int) LocalDate.of(2023, 6, 1).toEpochDay());
        Assertions.assertTrue(predicates.get(0).test(inside));
        Assertions.assertFalse(predicates.get(0).test(outside));
    }

    @Test
    public void testUnsupportedTypeSkipped() {
        // TIMESTAMP is not supported in v1; the record must be skipped, not fail.
        String encoded = record("ts", "minmax", "2024-01-01 00:00:00", "2024-12-31 00:00:00");
        List<Predicate> predicates = PaimonRuntimePredicates.parse(ROW_TYPE, encoded);
        Assertions.assertTrue(predicates.isEmpty());
    }

    @Test
    public void testUnknownColumnSkipped() {
        String encoded = record("no_such_col", "minmax", "1", "1");
        List<Predicate> predicates = PaimonRuntimePredicates.parse(ROW_TYPE, encoded);
        Assertions.assertTrue(predicates.isEmpty());
    }

    @Test
    public void testMalformedRecordSkipped() {
        String encoded = record("project_id", "minmax", "1", "1")
                + RECORD_SEP + "garbage"
                + RECORD_SEP + record("project_id", "minmax", "not_a_number", "5")
                + RECORD_SEP + record("project_id", "in");
        List<Predicate> predicates = PaimonRuntimePredicates.parse(ROW_TYPE, encoded);
        Assertions.assertEquals(1, predicates.size());
    }

    @Test
    public void testStringDecodeIsByteFaithful() {
        // A full-range string filter's upper bound is a 0xff sentinel (invalid UTF-8).
        // The decoded predicate must keep every string, including 4-byte UTF-8 values,
        // instead of corrupting the bound to U+FFFD and excluding them.
        String minB64 = Base64.getEncoder().encodeToString(new byte[0]);
        String maxB64 = Base64.getEncoder().encodeToString(
                new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff});
        String encoded = "email" + FIELD_SEP + "minmax" + FIELD_SEP + minB64 + FIELD_SEP + maxB64;
        List<Predicate> predicates = PaimonRuntimePredicates.parse(ROW_TYPE, encoded);
        Assertions.assertEquals(1, predicates.size());
        Assertions.assertTrue(predicates.get(0).test(row(1L, "a@b.com")));
        Assertions.assertTrue(predicates.get(0).test(row(1L, "😀emoji@x.com")));
    }

    @Test
    public void testNullOrEmptyInput() {
        Assertions.assertTrue(PaimonRuntimePredicates.parse(ROW_TYPE, null).isEmpty());
        Assertions.assertTrue(PaimonRuntimePredicates.parse(ROW_TYPE, "").isEmpty());
    }
}
