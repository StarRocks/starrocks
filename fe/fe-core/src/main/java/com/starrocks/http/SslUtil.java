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

package com.starrocks.http;

import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class SslUtil {

    private static final Logger LOG = LogManager.getLogger(SslUtil.class);
    private static final Pattern COMMA = Pattern.compile("\\s*,\\s*");

    private static List<Pattern> parsePatterns(String cfg) {
        if (cfg == null || cfg.isBlank()) {
            return Collections.emptyList();
        }
        return Arrays.stream(COMMA.split(cfg))
                .filter(s -> !s.isEmpty())
                .map(Pattern::compile)
                .toList();
    }

    private static boolean matchesAny(List<Pattern> ps, String s) {
        for (Pattern p : ps) {
            if (p.matcher(s).matches()) {
                return true;
            }
        }
        return false;
    }

    public static String[] filterCipherSuites(String[] defaults) {
        List<Pattern> whitelist = parsePatterns(Config.ssl_cipher_whitelist);
        List<Pattern> blacklist = parsePatterns(Config.ssl_cipher_blacklist);

        if (whitelist.isEmpty() && blacklist.isEmpty()) {
            return defaults;
        }

        Predicate<String> allow = whitelist.isEmpty() ? x -> true  : c -> matchesAny(whitelist, c);
        Predicate<String> deny  = blacklist.isEmpty() ? x -> false : c -> matchesAny(blacklist, c);

        String[] filtered = Arrays.stream(defaults == null ? new String[0] : defaults)
                .filter(Objects::nonNull)
                .filter(allow)
                .filter(deny.negate())
                .toArray(String[]::new);

        if (filtered.length == 0) {
            LOG.warn("No cipher suites left, please check your whitelist and blacklist settings.");
        }
        return filtered;
    }
}
