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

package com.starrocks.context;

/**
 * Shared SQL-literal escape helper for the semantic-context module. The four write paths
 * (ContextWriteExecutor / ChannelExecutor / WorkspaceObjectWriter / ContextReadExecutor) used to
 * each carry their own private {@code escapeSql} that only handled {@code '} (and sometimes
 * {@code \\}). That left several classes of inputs unsafe:
 *
 * <ul>
 *   <li>NUL bytes — StarRocks rejects strings containing {@code \\u0000}, and a NUL inside a
 *       user-supplied markdown body would abort the entire write with an opaque parser error;</li>
 *   <li>raw {@code \\r} / {@code \\n} / {@code \\t} — these don't break SQL semantics in StarRocks
 *       (the parser accepts them inside single-quoted literals), but a literal newline inside an
 *       audit-logged DML string breaks one-line-per-statement log readers and several downstream
 *       tools that {@code grep} stmts by row;</li>
 *   <li>other ASCII control bytes (0x01..0x08, 0x0b..0x1f, 0x7f) — encoded the same way so a
 *       binary blob accidentally passed through markdown ingestion cannot smuggle bytes that
 *       confuse the BE-side tokenizer or any external consumer of the logged SQL.</li>
 * </ul>
 *
 * <p>The escape is one-way (write-only): retrieval reads do not need to round-trip {@code \\t}
 * back to a tab because the column store holds the original byte sequence. Logs and {@code SHOW}
 * outputs are the only consumers of the escaped form.
 */
public final class ContextSqlEscape {

    private ContextSqlEscape() {
        // utility — not instantiable
    }

    /**
     * Returns the SQL literal form of {@code s} including the surrounding single quotes. A
     * {@code null} input returns the unquoted SQL keyword {@code NULL}.
     */
    public static String literal(String s) {
        if (s == null) {
            return "NULL";
        }
        return "'" + body(s) + "'";
    }

    /**
     * Returns the SQL literal form, or the unquoted keyword {@code NULL} when {@code s} is
     * null or empty. Most semantic-context tables treat empty strings the same as NULL, so the
     * two get folded for compactness in the emitted DML.
     */
    public static String literalOrNull(String s) {
        if (s == null || s.isEmpty()) {
            return "NULL";
        }
        return literal(s);
    }

    /**
     * Returns the escaped body without surrounding quotes — used by call sites that already
     * provide their own quoting (e.g. concatenating into a {@code PARSE_JSON('…')} call).
     */
    public static String body(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder out = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\0':
                    // Drop. StarRocks rejects NUL inside string columns; preserving it would
                    // abort the DML server-side with a confusing parser error.
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                case '\'':
                    out.append("''");
                    break;
                case '\r':
                    out.append("\\r");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                case '\t':
                    out.append("\\t");
                    break;
                default:
                    if (c < 0x20 || c == 0x7f) {
                        // Other ASCII control chars: emit a backslash-hex form so the SQL log
                        // line stays grep-friendly. Single character per encoded byte keeps
                        // line-by-line audit consumers happy.
                        out.append("\\x");
                        out.append(Integer.toHexString(c | 0x100).substring(1));
                    } else {
                        out.append(c);
                    }
                    break;
            }
        }
        return out.toString();
    }
}
