// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common;

import com.google.common.base.Strings;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.regex.Pattern;

// Wrap for Java pattern and matcher
public class PatternMatcher {
    private Pattern pattern;

    public boolean match(String candidate) {
        if (pattern == null || candidate == null) {
            // No pattern, how can I explain this? Return false now.
            // No candidate, return false.
            return false;
        }
        if (pattern.matcher(candidate).matches()) {
            return true;
        }
        return false;
    }

    /*
     * Mysql has only 2 patterns.
     * '%' to match any character sequence
     * '_' to match any single character.
     * So we convert '%' to '.*', and '_' to '.'
     *
     * eg:
     *      abc% -> abc.*
     *      ab_c -> ab.c
     *
     * We also need to handle escape character '\'.
     * User use '\' to escape reserved words like '%', '_', or '\' itself
     *
     * eg:
     *      abc%  -> abc.*
     *      ab_c  -> ab.c
     *      ab\%c -> matches ab%c
     *      ab\_c -> matches ab_c
     *      ab\\c -> matches ab\c
     *
     * We also have to ignore meaningless '\' like:'ab\c', convert it to 'abc'.
     * Literal segments are wrapped with {@link Pattern#quote} so regex metacharacters
     * (for example '(', ')', '+') in table or database names do not break compilation.
     */
    private static String convertMysqlPattern(String mysqlPattern) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < mysqlPattern.length(); ++i) {
            char ch = mysqlPattern.charAt(i);
            switch (ch) {
                case '%':
                    sb.append(".*");
                    break;
                case '_':
                    sb.append(".");
                    break;
                case '\\':
                    if (i + 1 < mysqlPattern.length()) {
                        char next = mysqlPattern.charAt(i + 1);
                        if (next == '%' || next == '_') {
                            // \% or \_ → literal char (not special in regex)
                            sb.append(next);
                            i++;
                        } else if (next == '\\') {
                            // \\ → literal backslash → regex needs "\\\\"
                            sb.append("\\\\");
                            i++;
                        } else {
                            // meaningless \, ignore
                        }
                    } else {
                        // trailing \, treat as literal backslash
                        sb.append("\\\\");
                    }
                    break;
                default:
                    // escape regex-special characters
                    if (".[]{}()*+?^$|".indexOf(ch) >= 0) {
                        sb.append('\\');
                    }
                    sb.append(ch);
                    break;
            }
        }
        return sb.toString();
    }

    public static PatternMatcher createMysqlPattern(String mysqlPattern, boolean caseSensitive) {
        PatternMatcher matcher = new PatternMatcher();

        // Match nothing
        String newMysqlPattern = Strings.nullToEmpty(mysqlPattern);

        String javaPattern = convertMysqlPattern(newMysqlPattern);
        try {
            if (caseSensitive) {
                matcher.pattern = Pattern.compile(javaPattern);
            } else {
                matcher.pattern = Pattern.compile(javaPattern, Pattern.CASE_INSENSITIVE);
            }
        } catch (Exception e) {
            throw new SemanticException("Bad pattern in SQL: " + e.getMessage());
        }
        return matcher;
    }

    /**
     * Escape a literal value so it can be used as a MySQL LIKE pattern that
     * matches the value exactly. The three LIKE-special characters {@code \},
     * {@code %} and {@code _} are each prefixed with a backslash.
     */
    public static String escapeLikeValue(String value) {
        if (value == null) {
            return null;
        }
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    public static boolean matchPattern(String pattern, String tableName, PatternMatcher matcher,
                                        boolean caseSensitive) {
        if (matcher != null && !matcher.match(tableName)) {
            if (caseSensitive && !tableName.equals(pattern)) {
                return false;
            } else if (!caseSensitive && !tableName.equalsIgnoreCase(pattern)) {
                return false;
            }
        }
        return true;
    }

}
