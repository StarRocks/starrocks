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

package com.starrocks.common.util;

import java.util.Arrays;
import java.util.stream.Collectors;

public class LogUtil {
    public static String getCurrentStackTrace() {
        return Arrays.stream(Thread.currentThread().getStackTrace())
                .map(stack -> "        " + stack.toString())
                .collect(Collectors.joining(System.lineSeparator(), System.lineSeparator(), ""));
    }

    public static String removeCommentAndLineSeparator(String origStmt) {
        char inStringStart = '-';

        StringBuilder sb = new StringBuilder();

        int idx = 0;
        int length = origStmt.length();
        while (idx < length) {
            char character = origStmt.charAt(idx);

            if (character == '\"' || character == '\'' || character == '`') {
                // process quote string
                inStringStart = character;
                appendChar(sb, inStringStart);
                idx++;
                while (idx < length && ((origStmt.charAt(idx) != inStringStart) || origStmt.charAt(idx - 1) == '\\')) {
                    sb.append(origStmt.charAt(idx));
                    ++idx;
                }
                sb.append(inStringStart);
            } else if ((character == '-' && idx != length - 1 && origStmt.charAt(idx + 1) == '-') ||
                    character == '#') {
                // process comment style like '-- comment' or '# comment'
                while (idx < length - 1 && origStmt.charAt(idx) != '\n') {
                    ++idx;
                }
                appendChar(sb, ' ');
            } else if (character == '/' && idx != length - 2 &&
                    origStmt.charAt(idx + 1) == '*' && origStmt.charAt(idx + 2) != '+') {
                //  process comment style like '/* comment */'
                while (idx < length - 1 && (origStmt.charAt(idx) != '*' || origStmt.charAt(idx + 1) != '/')) {
                    ++idx;
                }
                ++idx;
                appendChar(sb, ' ');
            } else if (character == '/' && idx != origStmt.length() - 2 &&
                    origStmt.charAt(idx + 1) == '*' && origStmt.charAt(idx + 2) == '+') {
                //  process hint
                while (idx < length - 1 && (origStmt.charAt(idx) != '*' || origStmt.charAt(idx + 1) != '/')) {
                    appendChar(sb, origStmt.charAt(idx));
                    idx++;
                }
                appendChar(sb, '*');
                appendChar(sb, '/');
                idx++;
            } else if (character == '\t' || character == '\r' || character == '\n') {
                // replace line separator
                appendChar(sb, ' ');
            } else {
                // append normal character
                appendChar(sb, character);
            }

            idx++;
        }
        return sb.toString();
    }

    private static void appendChar(StringBuilder sb, char character) {
        if (character != ' ') {
            sb.append(character);
        } else if (sb.length() > 0 && sb.charAt(sb.length() - 1) != ' ') {
            sb.append(" ");
        }
    }
}
