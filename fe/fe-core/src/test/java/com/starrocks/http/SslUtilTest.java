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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SslUtilTest {

    private static final String[] DEFAULTS = {
            // TLS 1.3
            "TLS_AES_128_GCM_SHA256", "TLS_AES_256_GCM_SHA384", "TLS_CHACHA20_POLY1305_SHA256",
            // TLS 1.2
            "ECDHE-RSA-AES128-GCM-SHA256", "ECDHE-RSA-AES256-GCM-SHA384",
            "ECDHE-RSA-CHACHA20-POLY1305", "AES128-GCM-SHA256", "AES256-GCM-SHA384",
            "AES128-SHA", "AES256-SHA"
    };

    @AfterEach
    void resetConfig() {
        Config.ssl_cipher_whitelist = "";
        Config.ssl_cipher_blacklist = "";
    }

    @Test
    void whitelistRegexOnlyGcmEcdheRsa() {
        Config.ssl_cipher_whitelist = "^ECDHE-RSA-.*-GCM-.*$";
        String[] out = SslUtil.filterCipherSuites(DEFAULTS);
        assertArrayEquals(
                new String[] {"ECDHE-RSA-AES128-GCM-SHA256", "ECDHE-RSA-AES256-GCM-SHA384"},
                out
        );
    }

    @Test
    void blacklistShaCbc() {
        Config.ssl_cipher_blacklist = ".*-SHA$";
        String[] out = SslUtil.filterCipherSuites(DEFAULTS);
        assertFalse(Arrays.asList(out).contains("AES128-SHA"));
        assertFalse(Arrays.asList(out).contains("AES256-SHA"));

        assertTrue(Arrays.asList(out).contains("AES128-GCM-SHA256"));
        assertTrue(Arrays.asList(out).contains("AES256-GCM-SHA384"));
    }

    @Test
    void excludeAllTls13() {
        Config.ssl_cipher_blacklist = "TLS_AES_128_GCM_SHA256,TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256";
        String[] out = SslUtil.filterCipherSuites(DEFAULTS);
        for (String s : out) {
            assertFalse(s.startsWith("TLS_"));
        }
    }

    @Test
    void whitelistSingleTls13() {
        Config.ssl_cipher_whitelist = "TLS_AES_128_GCM_SHA256";
        String[] out = SslUtil.filterCipherSuites(DEFAULTS);
        assertArrayEquals(new String[] {"TLS_AES_128_GCM_SHA256"}, out);
    }
}
