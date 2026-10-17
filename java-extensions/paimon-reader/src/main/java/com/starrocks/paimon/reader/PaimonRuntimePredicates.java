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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Decodes runtime-filter-derived predicates sent by the BE JNI scanner
 * ("runtime_predicate_info" param) into Paimon {@link Predicate}s.
 *
 * <p>Wire format (produced by {@code JniScanner::update_jni_scanner_params} in the BE):
 * records separated by {@code \u0003}; fields within a record separated by {@code \u0001}.
 * A record is {@code <column>\u0001<op>\u0001<v1>[\u0001<v2>...]} where {@code op} is
 * {@code minmax} (exactly two values, both bounds inclusive) or {@code in} (one or more
 * values). Values are Base64-encoded: strings as their raw column bytes (not
 * necessarily valid UTF-8), integers as ASCII decimal, dates as ASCII ISO-8601
 * ({@code YYYY-MM-DD}).
 *
 * <p>These predicates are best-effort hints: the BE re-applies the originating
 * conjuncts and runtime filters row-wise after the scan, so silently skipping an
 * unparseable or unsupported record is always safe. A record must therefore never
 * make the scan fail.
 */
public final class PaimonRuntimePredicates {

    private static final Logger LOG = LogManager.getLogger(PaimonRuntimePredicates.class);

    private static final String FIELD_SEP = "\u0001";
    private static final String RECORD_SEP = "\u0003";
    private static final String OP_MINMAX = "minmax";
    private static final String OP_IN = "in";

    private PaimonRuntimePredicates() {
    }

    public static List<Predicate> parse(RowType rowType, String encoded) {
        List<Predicate> result = new ArrayList<>();
        if (encoded == null || encoded.isEmpty()) {
            return result;
        }
        PredicateBuilder builder = new PredicateBuilder(rowType);
        List<String> fieldNames = rowType.getFieldNames();
        for (String record : encoded.split(RECORD_SEP)) {
            try {
                Predicate p = parseRecord(builder, rowType, fieldNames, record);
                if (p != null) {
                    result.add(p);
                }
            } catch (Exception e) {
                LOG.warn("Skipping undecodable runtime predicate record: {}", e.getMessage());
            }
        }
        return result;
    }

    private static Predicate parseRecord(PredicateBuilder builder, RowType rowType,
                                         List<String> fieldNames, String record) {
        String[] parts = record.split(FIELD_SEP, -1);
        if (parts.length < 3) {
            return null;
        }
        String column = parts[0];
        String op = parts[1];
        int idx = fieldNames.indexOf(column);
        if (idx < 0) {
            return null;
        }
        DataType type = rowType.getTypeAt(idx);
        if (OP_MINMAX.equals(op)) {
            if (parts.length != 4) {
                return null;
            }
            Object min = convertLiteral(type, decode(parts[2]));
            Object max = convertLiteral(type, decode(parts[3]));
            if (min == null || max == null) {
                return null;
            }
            return PredicateBuilder.and(
                    builder.greaterOrEqual(idx, min),
                    builder.lessOrEqual(idx, max));
        } else if (OP_IN.equals(op)) {
            List<Object> literals = new ArrayList<>(parts.length - 2);
            for (int i = 2; i < parts.length; i++) {
                Object literal = convertLiteral(type, decode(parts[i]));
                if (literal == null) {
                    return null;
                }
                literals.add(literal);
            }
            if (literals.isEmpty()) {
                return null;
            }
            return builder.in(idx, literals);
        }
        return null;
    }

    private static byte[] decode(String base64) {
        return Base64.getDecoder().decode(base64);
    }

    /** Returns null for types this decoder does not support; callers drop the record. */
    private static Object convertLiteral(DataType type, byte[] raw) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                // Must stay byte-faithful: the BE serializes raw column bytes, which may
                // not be valid UTF-8 (and full-range string filters use 0xff sentinels).
                // Round-tripping through java.lang.String would substitute U+FFFD and
                // could wrongly exclude valid rows.
                return BinaryString.fromBytes(raw);
            default:
                break;
        }
        String value = new String(raw, StandardCharsets.US_ASCII);
        switch (type.getTypeRoot()) {
            case TINYINT:
                return Byte.parseByte(value);
            case SMALLINT:
                return Short.parseShort(value);
            case INTEGER:
                return Integer.parseInt(value);
            case BIGINT:
                return Long.parseLong(value);
            case DATE:
                return (int) LocalDate.parse(value).toEpochDay();
            default:
                return null;
        }
    }
}
