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

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Objects;

import static org.joda.time.DateTimeConstants.SECONDS_PER_DAY;
import static org.joda.time.DateTimeConstants.SECONDS_PER_HOUR;
import static org.joda.time.DateTimeConstants.SECONDS_PER_MINUTE;

public abstract class DurationStyle {
    public static final DurationStyle LONG = new LongStyle();
    public static final DurationStyle ISO8601 = new ISO8601Style();

    public abstract Duration parse(String text);

    public abstract String toString(Duration duration);

    private static class ISO8601Style extends DurationStyle {

        @Override
        public Duration parse(String text) {
            return Duration.parse(text);
        }

        @Override
        public String toString(Duration duration) {
            return duration.toString();
        }
    }

    private static class LongStyle extends DurationStyle {

        @Override
        public Duration parse(String text) {
            Objects.requireNonNull(text, "text is null");
            String[] tokens = text.trim().split("\\s+");
            if ((tokens.length == 0) || (tokens.length % 2 != 0)) {
                throw new DateTimeParseException("Cannot parse text to Duration", text, 0);
            }
            long seconds = 0;
            for (int i = 0; i < tokens.length; i += 2) {
                try {
                    long number = Long.parseLong(tokens[i]);
                    String unit = tokens[i + 1].toLowerCase();
                    switch (unit) {
                        case "second":
                        case "seconds":
                            seconds += number;
                            break;
                        case "minute":
                        case "minutes":
                            seconds += number * SECONDS_PER_MINUTE;
                            break;
                        case "hour":
                        case "hours":
                            seconds += number * SECONDS_PER_HOUR;
                            break;
                        case "day":
                        case "days":
                            seconds += number * SECONDS_PER_DAY;
                            break;
                        default:
                            throw new DateTimeParseException("Cannot parse text to Duration", text, 0);
                    }
                } catch (NumberFormatException ignored) {
                    throw new DateTimeParseException("Cannot parse text to Duration", text, 0);
                }
            }
            return Duration.ofSeconds(seconds);
        }

        @Override
        public String toString(Duration duration) {
            if (duration.isZero()) {
                return "0 second";
            }
            long seconds = duration.getSeconds();
            long hours = seconds / SECONDS_PER_HOUR;
            int minutes = (int) ((seconds % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
            int secs = (int) (seconds % SECONDS_PER_MINUTE);
            StringBuilder buf = new StringBuilder();
            if (hours != 0) {
                buf.append(hours).append(" hours ");
            }
            if (minutes != 0) {
                buf.append(minutes).append(" minutes ");
            }
            if (secs != 0) {
                buf.append(secs).append(" seconds");
            }
            return buf.toString().trim();
        }
    }
}
