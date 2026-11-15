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

import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.jwk.source.RefreshAheadCachingJWKSetSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.DefaultResourceRetriever;
import com.nimbusds.jose.util.Resource;
import com.nimbusds.jose.util.ResourceRetriever;
import com.starrocks.common.Config;
import com.starrocks.metric.MetricRepo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.ParseException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class JwkMgr {
    private static final Logger LOG = LogManager.getLogger(JwkMgr.class);

    /**
     * Cached, per-URL {@link JWKSource}. HTTP Bearer does a full authentication on every request,
     * so an uncached fetch per request would hammer the IdP; the source handles TTL, refresh-ahead,
     * rate limiting and outage tolerance internally (see the {@code jwks_cache_*} configs).
     */
    private final ConcurrentHashMap<String, JWKSource<SecurityContext>> jwkSources = new ConcurrentHashMap<>();

    /**
     * Upper bound on the number of cached sources. The key carries a config fingerprint, so in
     * theory the map could grow with every config change; in practice both the URL count and the
     * change rate are tiny, and clearing the whole map on overflow is cheaper than an eviction
     * policy.
     */
    private static final int MAX_CACHED_SOURCES = 64;

    /**
     * Sources built from a local {@code conf/} key file, keyed by path plus the file's mtime. A new
     * mtime misses the entry and rebuilds, which is how an edited key file takes effect without a
     * restart.
     */
    private final ConcurrentHashMap<String, JWKSource<SecurityContext>> localSources = new ConcurrentHashMap<>();

    /**
     * Last-modified time of a local key file, or {@code "missing"} when it cannot be stat'ed.
     * Editing a key file moves its mtime, which is all this needs to notice. Not a content hash:
     * hashing would mean reading the file on every authentication, the very cost this cache exists
     * to avoid.
     */
    private String localFileStamp(String jwksUrl) {
        File f = localKeyFile(jwksUrl);
        return f.exists() ? String.valueOf(f.lastModified()) : "missing";
    }

    /** Where a non-URL {@code jwks_url} lives: a file under {@code conf/}. Tests point it elsewhere. */
    protected File localKeyFile(String jwksUrl) {
        return new File(Config.STARROCKS_HOME_DIR + "/conf/" + jwksUrl);
    }

    /**
     * Load a {@link JWKSet} directly (uncached). Kept for the OAuth2 MySQL browser-flow callback,
     * which runs at most once per login and does not need the cache.
     */
    public JWKSet getJwkSet(String jwksUrl) throws IOException, ParseException {
        InputStream jwksInputStream = null;
        try {
            if (jwksUrl.startsWith("http://") || jwksUrl.startsWith("https://")) {
                jwksInputStream = new URL(jwksUrl).openStream();
            } else {
                jwksInputStream = new FileInputStream(localKeyFile(jwksUrl));
            }
            return JWKSet.load(jwksInputStream);
        } finally {
            if (jwksInputStream != null) {
                jwksInputStream.close();
            }
        }
    }

    /**
     * Return a {@link JWKSource} for the given JWKS URL, or for a key file under {@code conf/}.
     *
     * <p>Two semantics that are easy to get wrong and hard to notice once wrong:
     * <ul>
     *   <li><b>A local {@code conf/} file is cached by its last-modified time, never by path
     *       alone.</b> Before this cache existed, every authentication re-read the file, so
     *       replacing the public key took effect immediately. Keying on the path alone would hand
     *       back the same {@code ImmutableJWKSet} forever and silently turn that into "restart the
     *       FE to pick up a new key" - a regression on the MySQL JWT path that no test would
     *       catch. The mtime in the key is what preserves edit-and-it-works while still reading
     *       the file once per edit rather than once per login.</li>
     *   <li><b>The cache key carries the {@code jwks_cache_*} config fingerprint.</b> A built
     *       {@code JWKSource} freezes the TTL / refresh / rate-limit values it was built with, so
     *       keying on the URL alone would make those {@code mutable} configs a lie: changing them
     *       would only affect URLs seen for the first time afterwards.</li>
     * </ul>
     */
    public JWKSource<SecurityContext> getJwkSource(String jwksUrl) {
        if (!isRemoteJwksUrl(jwksUrl)) {
            // Local conf/ file: keyed by the file's last-modified time, so editing the key file
            // still takes effect on the next authentication, but a steady stream of requests does
            // not re-read and re-parse it every time. HTTP Bearer authenticates per request, so
            // "every time" would mean once per request.
            if (localSources.size() >= MAX_CACHED_SOURCES) {
                localSources.clear();
            }
            return localSources.computeIfAbsent(jwksUrl + '|' + localFileStamp(jwksUrl),
                    key -> buildJwkSource(jwksUrl));
        }
        if (jwkSources.size() >= MAX_CACHED_SOURCES) {
            jwkSources.clear();
        }
        return jwkSources.computeIfAbsent(jwksUrl + '|' + cacheConfigFingerprint(), key -> buildJwkSource(jwksUrl));
    }

    /**
     * Only {@code http://} / {@code https://} reach a remote fetch (minimal SSRF guard); every
     * other scheme falls through to the local-file branch and resolves under {@code conf/}.
     */
    private static boolean isRemoteJwksUrl(String jwksUrl) {
        return jwksUrl.startsWith("http://") || jwksUrl.startsWith("https://");
    }

    /**
     * Fingerprint of the four {@code jwks_cache_*} configs. It is part of the cache key, which is
     * how {@code ADMIN SET FRONTEND CONFIG} takes effect: the next authentication misses the old
     * entry and rebuilds the source with the new values, and the unreferenced old source is
     * collected.
     */
    private static String cacheConfigFingerprint() {
        return Config.jwks_cache_ttl_seconds + ":" + Config.jwks_cache_refresh_ahead_seconds + ":"
                + Config.jwks_cache_refresh_rate_limit_seconds + ":" + Config.jwks_cache_outage_ttl_seconds;
    }

    protected JWKSource<SecurityContext> buildJwkSource(String jwksUrl) {
        try {
            if (isRemoteJwksUrl(jwksUrl)) {
                // Minimal SSRF guard: only http/https reach a remote fetch; other schemes fall
                // through to the local-file branch below and resolve under conf/. A host allowlist
                // is a later enhancement.
                // The listeners only log. The fetch metrics are taken in the retriever, the one
                // point every fetch passes through; see MetricRepo.COUNTER_JWKS_REFRESH for why the
                // refresh events are not used for counting.
                return JWKSourceBuilder.<SecurityContext>create(new URL(jwksUrl), new MeteredResourceRetriever())
                        .cache(Config.jwks_cache_ttl_seconds * 1000L, JWKSourceBuilder.DEFAULT_CACHE_REFRESH_TIMEOUT)
                        .refreshAheadCache(Config.jwks_cache_refresh_ahead_seconds * 1000L, true, event -> {
                            if (event instanceof RefreshAheadCachingJWKSetSource.UnableToRefreshAheadOfExpirationEvent) {
                                LOG.warn("JWKS refresh-ahead for {} did not finish before the cached keys expired; "
                                        + "the next authentication will block on a fetch", jwksUrl);
                            }
                        })
                        .rateLimited(Config.jwks_cache_refresh_rate_limit_seconds * 1000L, event ->
                                LOG.warn("JWKS refresh for {} was rate limited (jwks_cache_refresh_rate_limit_seconds={}); "
                                        + "many tokens with unknown kids are arriving", jwksUrl,
                                        Config.jwks_cache_refresh_rate_limit_seconds))
                        .outageTolerant(Config.jwks_cache_outage_ttl_seconds * 1000L, event ->
                                LOG.warn("JWKS endpoint {} is unreachable; authenticating with the cached keys for up to "
                                        + "jwks_cache_outage_ttl_seconds={}s", jwksUrl, Config.jwks_cache_outage_ttl_seconds))
                        .retrying(true)
                        .build();
            } else {
                // Local key file under conf/. Cached per file stamp by getJwkSource(), so this read
                // happens once per edit rather than once per authentication.
                return new ImmutableJWKSet<>(getJwkSet(jwksUrl));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to build JWK source for " + jwksUrl + ": " + e.getMessage(), e);
        }
    }

    /**
     * The one point every JWKS fetch passes through, whatever triggered it, so the place the fetch
     * metrics are taken. Same timeouts and size limit as the retriever the builder creates on its
     * own, so wrapping changes nothing about the fetch itself.
     */
    private static final class MeteredResourceRetriever implements ResourceRetriever {
        private final ResourceRetriever delegate = new DefaultResourceRetriever(
                JWKSourceBuilder.DEFAULT_HTTP_CONNECT_TIMEOUT, JWKSourceBuilder.DEFAULT_HTTP_READ_TIMEOUT,
                JWKSourceBuilder.DEFAULT_HTTP_SIZE_LIMIT);

        @Override
        public Resource retrieveResource(URL url) throws IOException {
            long start = System.nanoTime();
            String result = "failure";
            try {
                Resource resource = delegate.retrieveResource(url);
                result = "success";
                return resource;
            } finally {
                if (MetricRepo.hasInit) {
                    MetricRepo.HISTO_JWKS_FETCH_LATENCY_MS.update(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                    MetricRepo.COUNTER_JWKS_REFRESH.getMetric(result).increase(1L);
                }
            }
        }
    }
}
