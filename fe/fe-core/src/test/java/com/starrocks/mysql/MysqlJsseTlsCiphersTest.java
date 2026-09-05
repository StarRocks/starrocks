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

package com.starrocks.mysql;

import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MysqlJsseTlsCiphersTest {

    private static boolean containsTls12(String s) {
        return s.contains("_WITH_");
    }

    private static List<String> enabled(SSLEngine eng, String... suites) {
        SSLParameters p = new SSLParameters();
        p.setCipherSuites(suites);
        eng.setSSLParameters(p);
        return Arrays.asList(eng.getEnabledCipherSuites());
    }

    private static SSLEngine newEngine() throws Exception {
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, null, new SecureRandom());
        return ctx.createSSLEngine();
    }

    @Test
    void jsseOnlyTls13OneEnabledNoTls12() throws Exception {
        SSLEngine eng = newEngine();
        List<String> en = enabled(eng, "TLS_AES_128_GCM_SHA256");
        assertTrue(en.contains("TLS_AES_128_GCM_SHA256"));
        assertFalse(en.contains("TLS_AES_256_GCM_SHA384"));
        assertFalse(en.contains("TLS_CHACHA20_POLY1305_SHA256"));
        assertTrue(en.stream().noneMatch(MysqlJsseTlsCiphersTest::containsTls12));
    }

    @Test
    void jsseOnlyTls13allThreeExactlyThoseThreeNoTls12() throws Exception {
        SSLEngine eng = newEngine();
        String[] req = {
                "TLS_AES_128_GCM_SHA256",
                "TLS_AES_256_GCM_SHA384",
                "TLS_CHACHA20_POLY1305_SHA256"
        };
        List<String> en = enabled(eng, req);
        assertEquals(new HashSet<>(Arrays.asList(req)), new HashSet<>(en));
        assertTrue(en.stream().noneMatch(MysqlJsseTlsCiphersTest::containsTls12));
    }

    @Test
    void jsseOnlyTls12MultipleExactlyRequestedNoTls13() throws Exception {
        SSLEngine eng = newEngine();
        String[] req = {
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
        };
        List<String> en = enabled(eng, req);
        assertEquals(new HashSet<>(Arrays.asList(req)), en.stream().filter(MysqlJsseTlsCiphersTest::containsTls12)
                .collect(Collectors.toSet()));
        assertFalse(en.contains("TLS_AES_128_GCM_SHA256"));
        assertFalse(en.contains("TLS_AES_256_GCM_SHA384"));
        assertFalse(en.contains("TLS_CHACHA20_POLY1305_SHA256"));
    }

    @Test
    void jsseMixedSubsetExactUnionNoExtras() throws Exception {
        SSLEngine eng = newEngine();
        String[] req = {
                "TLS_AES_128_GCM_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
        };
        List<String> en = enabled(eng, req);
        assertEquals(new HashSet<>(Arrays.asList(req)), new HashSet<>(en));
    }

    @Test
    void jsseEmptyListResultsInNoEnabledSuites() throws Exception {
        SSLEngine eng = newEngine();
        List<String> en = enabled(eng);
        assertEquals(0, en.size());
    }
}