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

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CdcErrorUtils {
    private static final String ROW_DELETE_DETAIL = "does not support delete";
    private static final String CAPTURE_DISABLED_DETAIL = "change data capture was not enabled";

    private static final Pattern HEADER = Pattern.compile(
            "CDC-ERROR-([1-9][0-9]*) \\(([A-Z][A-Z0-9_]*)\\): ");

    private CdcErrorUtils() {
    }

    public static Optional<Parsed> find(String text) {
        if (text == null) {
            return Optional.empty();
        }
        Matcher matcher = HEADER.matcher(text);
        while (matcher.find()) {
            String codeText = matcher.group(1);
            String symbol = matcher.group(2);
            String message = text.substring(matcher.end());

            final int value;
            try {
                value = Integer.parseInt(codeText);
            } catch (NumberFormatException e) {
                continue;
            }
            TCdcErrorCode code = TCdcErrorCode.findByValue(value);
            if (code == null || code == TCdcErrorCode.UNKNOWN || !code.name().equals(symbol)) {
                continue;
            }
            return Optional.of(new Parsed(code, message));
        }
        return Optional.empty();
    }

    public static boolean isChangeNotTrackable(String message) {
        return find(message)
                .map(parsed -> parsed.getCode() == TCdcErrorCode.CHANGE_NOT_TRACKABLE)
                .orElse(false);
    }

    /**
     * Whether the rejection is the delete one, out of everything CHANGE_NOT_TRACKABLE covers. The backend
     * formats this detail in changes_connector.cpp; reword it there and this stops matching, which
     * CdcErrorUtilsTest pins.
     */
    public static boolean isRowDeleteRejection(String message) {
        return hasDetail(message, ROW_DELETE_DETAIL);
    }

    /**
     * Whether the rejection is a version published while change data capture was off -- the one condition
     * under CHANGE_NOT_TRACKABLE an operator caused and can undo, as opposed to a broken invariant.
     */
    public static boolean isCaptureDisabledRejection(String message) {
        return hasDetail(message, CAPTURE_DISABLED_DETAIL);
    }

    private static boolean hasDetail(String message, String detail) {
        return find(message).map(parsed -> parsed.getMessage().contains(detail)).orElse(false);
    }

    public static final class Parsed {
        private final TCdcErrorCode code;
        private final String message;

        private Parsed(TCdcErrorCode code, String message) {
            this.code = code;
            this.message = message;
        }

        public TCdcErrorCode getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }
    }
}
