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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.thrift.TUnit;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is used for reconstruct RuntimeProfile from plain text
 */
public class RuntimeProfileParser {
    private static final Pattern NONE_COUNTER_PATTERN =
            Pattern.compile("^- (.*?): $");
    private static final Pattern BYTE_COUNTER_PATTERN =
            Pattern.compile("^- (.*?): (-?\\d+\\.\\d+) (KB|MB|GB|TB|B)$");
    private static final Pattern BYTE_PER_SEC_COUNTER_PATTERN =
            Pattern.compile("^- (.*?): (-?\\d+\\.\\d+) (KB|MB|GB|TB|B)/sec$");
    private static final Pattern UNIT_COUNTER_PATTERN =
            Pattern.compile("^- (.*?): (-?\\d+(\\.\\d+)?([eE][-+]?\\d+)?)[BMK]?( \\((-?\\d+(\\.\\d+)?)\\))?$");
    private static final Pattern UNIT_PER_SEC_COUNTER_PATTERN =
            Pattern.compile("^- (.*?): (-?\\d+(\\.\\d+)?([eE][-+]?\\d+)?)[BMK]?( \\((-?\\d+(\\.\\d+)?)\\))? /sec$");
    private static final Pattern TIMER_PATTERN =
            Pattern.compile("^- (.*?): (((-?\\d+(\\.\\d+)?)(ms|us|ns|h|m|s))+)$");
    private static final Pattern TIMER_SEGMENT_PATTERN =
            Pattern.compile("(-?\\d+(\\.\\d+)?)(ms|us|ns|h|m|s)");
    private static final Pattern INFO_KV_STRING_PATTERN =
            Pattern.compile("^- (.*?): (.*?)$");
    private static final Pattern INFO_KEY_STRING_PATTERN =
            Pattern.compile("^- (.*?)$");

    public static RuntimeProfile parseFrom(String content) {
        BufferedReader bufferedReader = new BufferedReader(new StringReader(content));
        // (profile, profileIndent, counterStack(name, counter, counterIndent))
        LinkedList<ProfileTuple> profileStack = Lists.newLinkedList();
        RuntimeProfile root = null;
        try {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                int curIndent = getIndentWidth(line);
                String item = line.substring(curIndent);
                if (StringUtils.isBlank(item)) {
                    continue;
                }
                String profileName;
                CounterTuple counterTuple;
                InfoStringTuple infoStringTuple;
                if ((profileName = tryGetProfileName(item)) != null) {
                    RuntimeProfile profile = new RuntimeProfile(profileName);
                    if (root == null) {
                        root = profile;
                    }

                    while (!profileStack.isEmpty() && profileStack.peek().indent >= curIndent) {
                        profileStack.pop();
                    }

                    if (!profileStack.isEmpty()) {
                        profileStack.peek().profile.addChild(profile);
                    }

                    profileStack.push(new ProfileTuple(profile, curIndent));
                } else if ((counterTuple = tryParseCounter(item)) != null) {
                    Preconditions.checkState(!profileStack.isEmpty());
                    ProfileTuple profileTuple = profileStack.peek();

                    while (!profileTuple.counterStack.isEmpty() &&
                            profileTuple.counterStack.peek().indent >= curIndent) {
                        profileTuple.counterStack.pop();
                    }

                    Counter counter;
                    RuntimeProfile profile = profileTuple.profile;
                    if (profileTuple.counterStack.isEmpty()) {
                        counter = profile.addCounter(counterTuple.name, counterTuple.type, null);
                        counter.setValue(counterTuple.value);
                    } else {
                        String parent = profileTuple.counterStack.peek().name;
                        counter = profile.addCounter(counterTuple.name, counterTuple.type, null, parent);
                        counter.setValue(counterTuple.value);
                    }

                    counterTuple.indent = curIndent;
                    profileTuple.counterStack.push(counterTuple);
                } else if ((infoStringTuple = tryParseInfoString(item)) != null) {
                    Preconditions.checkState(!profileStack.isEmpty());
                    ProfileTuple profileTuple = profileStack.peek();
                    profileTuple.profile.addInfoString(infoStringTuple.name, infoStringTuple.content);
                }
            }
        } catch (IOException e) {
            return null;
        }

        return root;
    }

    private static int getIndentWidth(String line) {
        for (int i = 0; i < line.length(); i++) {
            if (!Character.isWhitespace(line.charAt(i))) {
                return i;
            }
        }
        return line.length();
    }

    private static String tryGetProfileName(String item) {
        if (!item.startsWith("-") && item.endsWith(":")) {
            return item.substring(0, item.length() - 1);
        }
        return null;
    }

    private static CounterTuple tryParseCounter(String item) {
        CounterTuple counterTuple = tryParseByteCounter(item);
        if (counterTuple != null) {
            return counterTuple;
        }
        counterTuple = tryParseBytePerSecCounter(item);
        if (counterTuple != null) {
            return counterTuple;
        }
        counterTuple = tryParseUnitCounter(item);
        if (counterTuple != null) {
            return counterTuple;
        }
        counterTuple = tryParseUnitPerSecCounter(item);
        if (counterTuple != null) {
            return counterTuple;
        }
        counterTuple = tryParseTimer(item);
        if (counterTuple != null) {
            return counterTuple;
        }
        return tryParseNone(item);
    }

    private static InfoStringTuple tryParseInfoString(String item) {
        Matcher matcher = INFO_KV_STRING_PATTERN.matcher(item);
        if (!matcher.matches()) {
            matcher = INFO_KEY_STRING_PATTERN.matcher(item);
            if (!matcher.matches()) {
                return null;
            }
            String name = matcher.group(1);
            return new InfoStringTuple(name, "");
        }

        String name = matcher.group(1);
        String content = matcher.group(2);

        return new InfoStringTuple(name, content);
    }

    private static CounterTuple tryParseByteCounter(String item) {
        Matcher matcher = BYTE_COUNTER_PATTERN.matcher(item);
        if (!matcher.matches()) {
            return null;
        }

        String name = matcher.group(1);
        BigDecimal value = new BigDecimal(matcher.group(2));
        String unit = matcher.group(3);

        return new CounterTuple(name, TUnit.BYTES,
                value.multiply(new BigDecimal(Long.toString(getByteFactor(unit)))).longValue());
    }

    private static CounterTuple tryParseBytePerSecCounter(String item) {
        Matcher matcher = BYTE_PER_SEC_COUNTER_PATTERN.matcher(item);
        if (!matcher.matches()) {
            return null;
        }

        String name = matcher.group(1);
        BigDecimal value = new BigDecimal(matcher.group(2));
        String unit = matcher.group(3);

        return new CounterTuple(name, TUnit.BYTES_PER_SECOND,
                value.multiply(new BigDecimal(Long.toString(getByteFactor(unit)))).longValue());
    }

    private static long getByteFactor(String unit) {
        long factor = 1;
        if (unit != null) {
            switch (unit) {
                case "KB":
                    factor = DebugUtil.KILOBYTE;
                    break;
                case "MB":
                    factor = DebugUtil.MEGABYTE;
                    break;
                case "GB":
                    factor = DebugUtil.GIGABYTE;
                    break;
                case "TB":
                    factor = DebugUtil.TERABYTE;
                    break;
                default:
                    break;
            }
        }
        return factor;
    }

    private static CounterTuple tryParseUnitCounter(String item) {
        Matcher matcher = UNIT_COUNTER_PATTERN.matcher(item);
        if (!matcher.matches()) {
            return null;
        }

        String name = matcher.group(1);
        String value = matcher.group(2);
        String preciseValue = matcher.group(6);

        if (StringUtils.isNotBlank(preciseValue)) {
            return new CounterTuple(name, TUnit.UNIT, Long.parseLong(preciseValue));
        } else {
            long lValue = Long.parseLong(value);
            if (lValue > 1000) {
                return null;
            }
            return new CounterTuple(name, TUnit.UNIT, lValue);
        }
    }

    private static CounterTuple tryParseUnitPerSecCounter(String item) {
        Matcher matcher = UNIT_PER_SEC_COUNTER_PATTERN.matcher(item);
        if (!matcher.matches()) {
            return null;
        }

        String name = matcher.group(1);
        String value = matcher.group(2);
        String preciseValue = matcher.group(6);

        if (StringUtils.isNotBlank(preciseValue)) {
            return new CounterTuple(name, TUnit.UNIT_PER_SECOND, Long.parseLong(preciseValue));
        } else {
            long lValue = Long.parseLong(value);
            if (lValue > 1000) {
                return null;
            }
            return new CounterTuple(name, TUnit.UNIT_PER_SECOND, lValue);
        }
    }

    private static CounterTuple tryParseTimer(String item) {
        Matcher matcher = TIMER_PATTERN.matcher(item);
        if (!matcher.matches()) {
            return null;
        }

        String name = matcher.group(1);
        String timeSegments = matcher.group(2);
        Matcher segmentMatcher = TIMER_SEGMENT_PATTERN.matcher(timeSegments);

        BigDecimal total = new BigDecimal("0");
        while (segmentMatcher.find()) {
            BigDecimal value = new BigDecimal(segmentMatcher.group(1));
            String unit = segmentMatcher.group(3);

            long factor;
            switch (unit) {
                case "us":
                    factor = DebugUtil.THOUSAND;
                    break;
                case "ms":
                    factor = DebugUtil.MILLION;
                    break;
                case "s":
                    factor = DebugUtil.BILLION;
                    break;
                case "m":
                    factor = (long) DebugUtil.MINUTE * DebugUtil.MILLION;
                    break;
                case "h":
                    factor = (long) DebugUtil.HOUR * DebugUtil.MILLION;
                    break;
                default:
                    factor = 1;
            }

            total = total.add(value.multiply(new BigDecimal(Long.toString(factor))));
        }

        return new CounterTuple(name, TUnit.TIME_NS, total.longValue());
    }

    private static CounterTuple tryParseNone(String item) {
        Matcher matcher = NONE_COUNTER_PATTERN.matcher(item);
        if (!matcher.matches()) {
            return null;
        }
        String name = matcher.group(1);
        return new CounterTuple(name, TUnit.NONE, 0);
    }

    private static final class ProfileTuple {
        private final RuntimeProfile profile;
        private final int indent;
        private final LinkedList<CounterTuple> counterStack = Lists.newLinkedList();

        private ProfileTuple(RuntimeProfile profile, int indent) {
            this.profile = profile;
            this.indent = indent;
        }
    }

    private static final class CounterTuple {
        private final String name;
        private final TUnit type;
        private final long value;
        private int indent;

        private CounterTuple(String name, TUnit type, long value) {
            this.name = name;
            this.type = type;
            this.value = value;
        }
    }

    private static final class InfoStringTuple {
        private final String name;
        private final String content;

        private InfoStringTuple(String name, String content) {
            this.name = name;
            this.content = content;
        }
    }
}
