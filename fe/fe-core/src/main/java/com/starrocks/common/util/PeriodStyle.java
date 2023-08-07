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

import java.time.Period;
import java.time.format.DateTimeParseException;
import java.util.Objects;

public abstract class PeriodStyle {
    public static final PeriodStyle ISO8601 = new ISO8601Style();
    public static final PeriodStyle LONG = new LongStyle();

    /**
     * Obtains a Period from a text string.
     *
     * @param text the text to parse, not null
     * @return the parsed period, not null
     * @throws DateTimeParseException – if the text cannot be parsed to a period
     */
    public abstract Period parse(String text);

    public abstract String toString(Period period);

    public static PeriodStyle valueOf(String value) {
        if ("ISO8601".equalsIgnoreCase(value)) {
            return ISO8601;
        }
        if ("LONG".equalsIgnoreCase(value)) {
            return LONG;
        }
        return null;
    }

    private static class ISO8601Style extends PeriodStyle {

        @Override
        public Period parse(String text) {
            return Period.parse(text);
        }

        @Override
        public String toString(Period period) {
            return period.toString();
        }
    }

    private static class LongStyle extends PeriodStyle {

        @Override
        public Period parse(String text) {
            Objects.requireNonNull(text, "text is null");
            String[] tokens = text.trim().split("\\s+");
            if ((tokens.length == 0) || (tokens.length % 2 != 0)) {
                throw new DateTimeParseException("Cannot parse text to Period", text, 0);
            }
            int years = 0;
            int months = 0;
            int days = 0;
            for (int i = 0; i < tokens.length; i += 2) {
                try {
                    int number = Integer.parseInt(tokens[i]);
                    String unit = tokens[i + 1].toLowerCase();
                    switch (unit) {
                        case "day":
                        case "days":
                            days += number;
                            break;
                        case "week":
                        case "weeks":
                            days += 7 * number;
                            break;
                        case "month":
                        case "months":
                            months += number;
                            break;
                        case "year":
                        case "years":
                            years += number;
                            break;
                        default:
                            throw new DateTimeParseException("Cannot parse text to Period", text, 0);
                    }
                } catch (NumberFormatException ignored) {
                    throw new DateTimeParseException("Cannot parse text to Period", text, 0);
                }
            }
            return Period.of(years, months, days);
        }

        @Override
        public String toString(Period period) {
            if (period.isZero()) {
                return "0 day";
            } else {
                int years = period.getYears();
                int months = period.getMonths();
                int days = period.getDays();
                StringBuilder buf = new StringBuilder();
                if (years != 0) {
                    buf.append(years).append(" years ");
                }
                if (months != 0) {
                    buf.append(months).append(" months ");
                }
                if (days != 0) {
                    buf.append(days).append(" days");
                }
                return buf.toString().trim();
            }
        }
    }
}
