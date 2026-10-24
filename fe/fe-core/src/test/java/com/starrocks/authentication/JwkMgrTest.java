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

package com.starrocks.authentication;

import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.starrocks.common.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * How {@link JwkMgr} caches key sources. Building a remote source never contacts the URL, so an
 * unreachable address is fine here; the fetch behaviour itself is in {@link JwksCacheTest}.
 */
public class JwkMgrTest {
    private static final String REMOTE = "http://127.0.0.1:1/realms/master/protocol/openid-connect/certs";
    private int savedTtl;

    @BeforeEach
    public void setUp() {
        savedTtl = Config.jwks_cache_ttl_seconds;
    }

    @AfterEach
    public void tearDown() {
        Config.jwks_cache_ttl_seconds = savedTtl;
    }

    @Test
    public void testSameUrlSharesOneSource() {
        // TC-L1-44-01: HTTP Bearer authenticates on every request; the source, with its cache and
        // refresh schedule, has to be the same object each time or the cache is worthless.
        JwkMgr mgr = new JwkMgr();
        JWKSource<SecurityContext> first = mgr.getJwkSource(REMOTE);
        Assertions.assertSame(first, mgr.getJwkSource(REMOTE));
        Assertions.assertNotSame(first, mgr.getJwkSource(REMOTE + "?other"), "a different URL is a different source");
    }

    @Test
    public void testLocalFileBecomesAnImmutableSetAndOtherSchemesAreLocalToo() {
        // TC-L1-44-02 / TC-L2-44-07 (non-http half): only http:// and https:// are fetched. A bare
        // file name is read from conf/, and so is anything with another scheme - a file:// or ftp://
        // "URL" never reaches the network (minimal SSRF guard).
        JwkMgr mgr = new MockTokenUtils.MockJwkMgr();
        Assertions.assertTrue(mgr.getJwkSource("jwks.json") instanceof ImmutableJWKSet);

        RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                () -> mgr.getJwkSource("file:///etc/passwd"));
        // Resolved as a conf/ file that does not exist - and failing at build time is itself the
        // proof it was not treated as remote: a remote source is built lazily and never fails here.
        Assertions.assertTrue(e.getMessage().contains("Failed to build JWK source"), e.getMessage());
    }

    @Test
    public void testChangingACacheConfigRebuildsTheSource() {
        // TC-L2-44-09 / AC-18: the jwks_cache_* configs are mutable. A built source freezes the values
        // it was built with, so the config fingerprint is part of the cache key: change a value and
        // the next authentication gets a source built with the new one.
        JwkMgr mgr = new JwkMgr();
        JWKSource<SecurityContext> before = mgr.getJwkSource(REMOTE);
        Config.jwks_cache_ttl_seconds = savedTtl + 1;
        JWKSource<SecurityContext> after = mgr.getJwkSource(REMOTE);
        Assertions.assertNotSame(before, after, "a changed config must not keep serving the old source");
        Assertions.assertSame(after, mgr.getJwkSource(REMOTE), "and the rebuilt source is cached in turn");
    }
}
