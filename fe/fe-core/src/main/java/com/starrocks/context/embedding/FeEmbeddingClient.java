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

package com.starrocks.context.embedding;

import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Batched FE-side embedding caller. Posts one HTTP request to the configured provider
 * (e.g. OpenAI {@code /v1/embeddings}) with an array-input body and returns vectors in
 * input order. Used by {@code ContextWriteExecutor.upsertBatch} to precompute all
 * fragment embeddings before the fragments INSERT — bypassing the BE-side per-row
 * {@code embedding(text, parse_json(cfg))} path that fans out one HTTP per row.
 *
 * <p>Provider settings (endpoint / model / dimensions / timeout / api_key) come from the
 * default {@link AIProvider} (embedding type) registered via {@code CREATE EMBEDDING PROVIDER ...;
 * SET ... AS DEFAULT EMBEDDING PROVIDER}. The api_key value lives only in FE metadata —
 * never in fe.conf, an OS env var, or thrift fragment params logged outside the BE-side
 * embedding() call site.
 */
public final class FeEmbeddingClient {

    private static final Logger LOG = LogManager.getLogger(FeEmbeddingClient.class);

    /**
     * Max items per provider HTTP request. OpenAI {@code /v1/embeddings} accepts up to 2048
     * inputs per call, but practical token-per-request limits start to bite well before that
     * for non-trivial fragment sizes. 100 keeps each request comfortably under every documented
     * and undocumented limit (~15 k tokens at ~150 tok/fragment) while still amortizing TLS +
     * HTTP overhead across many vectors.
     */
    static final int MAX_BATCH_ITEMS = 100;

    /**
     * Total provider send attempts per chunk (1 initial + {@code MAX_SEND_ATTEMPTS - 1} retries).
     * The static {@link #HTTP} client keeps a connection pool alive for the FE process lifetime, so
     * a connection idle since the last bulk-import can be silently reaped by the peer/NAT. The first
     * {@code send} on that stale connection then throws {@code IOException: Connection reset}. The
     * JDK client evicts the dead connection on that failure, so simply retrying opens a fresh one —
     * which is why low-frequency clustered derives (long idle) hit this and high-frequency ones do
     * not. Embeddings are idempotent, so re-sending is safe.
     */
    static final int MAX_SEND_ATTEMPTS = 3;
    static final long RETRY_BACKOFF_MS = 200;

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private FeEmbeddingClient() {
    }

    /**
     * Embed every text in {@code texts}, chunking the input into ≤{@link #MAX_BATCH_ITEMS}-sized
     * provider HTTP calls. Returns a list with the same size and order as the input. Any chunk
     * failure (HTTP non-200, network error, malformed response) throws — callers see a single
     * {@link ContextException} and must treat the entire batch as failed. Earlier successful
     * chunks are NOT retained on partial failure; that is the price of the simple all-or-nothing
     * contract upstream callers (bulk-import) already rely on.
     */
    public static List<float[]> embedBatch(List<String> texts) {
        if (texts == null || texts.isEmpty()) {
            return new ArrayList<>();
        }
        // Resolve the DEFAULT EMBEDDING PROVIDER once for the whole batch and reuse it across every
        // chunk. Re-resolving per chunk would let a concurrent ALTER / SET DEFAULT EMBEDDING PROVIDER
        // switch models or dimensions mid-import, so a single returned batch could mix vectors from
        // different models/dimensions (and a dimension change fails the fixed-dim fragments write).
        EmbedConfig cfg = resolveConfig();
        int n = texts.size();
        if (n <= MAX_BATCH_ITEMS) {
            return embedOneChunk(cfg, texts);
        }
        List<float[]> out = new ArrayList<>(n);
        int chunkCount = (n + MAX_BATCH_ITEMS - 1) / MAX_BATCH_ITEMS;
        int chunkIdx = 0;
        for (int start = 0; start < n; start += MAX_BATCH_ITEMS) {
            int end = Math.min(start + MAX_BATCH_ITEMS, n);
            long t = System.nanoTime();
            List<float[]> chunkOut = embedOneChunk(cfg, texts.subList(start, end));
            long ms = (System.nanoTime() - t) / 1_000_000;
            LOG.info("FeEmbed chunk {}/{} items={} ms={}", ++chunkIdx, chunkCount, end - start, ms);
            out.addAll(chunkOut);
        }
        return out;
    }

    /**
     * Resolve the DEFAULT EMBEDDING PROVIDER's settings into an immutable snapshot. Called once per
     * batch so every chunk shares one model / dimension / endpoint even if the default provider is
     * altered or switched mid-import.
     */
    private static EmbedConfig resolveConfig() {
        AIProvider provider = GlobalStateMgr.getCurrentState().getAIProviderMgr()
                .getDefaultProvider(AIProviderType.EMBEDDING);
        if (provider == null) {
            throw new ContextException(ContextErrorCode.VECTOR_NOT_READY,
                    "no DEFAULT EMBEDDING PROVIDER configured",
                    "Run: CREATE EMBEDDING PROVIDER ...; SET <name> AS DEFAULT EMBEDDING PROVIDER");
        }
        String url = provider.getEndpoint();
        if (Strings.isNullOrEmpty(url)) {
            throw new ContextException(ContextErrorCode.VECTOR_NOT_READY,
                    "DEFAULT EMBEDDING PROVIDER '" + provider.getName() + "' has no endpoint set");
        }
        Integer dimObj = provider.getDimensions();
        Integer timeoutObj = provider.getTimeoutMs();
        long timeoutMs = timeoutObj != null && timeoutObj > 0 ? timeoutObj : 30000;
        return new EmbedConfig(url, provider.getModel(), dimObj == null ? 0 : dimObj, timeoutMs, provider.getApiKey());
    }

    /** Immutable snapshot of the resolved embedding provider settings for one batch. */
    private static final class EmbedConfig {
        final String url;
        final String model;
        final int dim;
        final long timeoutMs;
        final String apiKey;

        EmbedConfig(String url, String model, int dim, long timeoutMs, String apiKey) {
            this.url = url;
            this.model = model;
            this.dim = dim;
            this.timeoutMs = timeoutMs;
            this.apiKey = apiKey;
        }
    }

    /**
     * Single provider HTTP call. Caller must guarantee {@code texts.size() <= MAX_BATCH_ITEMS}.
     */
    private static List<float[]> embedOneChunk(EmbedConfig cfg, List<String> texts) {
        String url = cfg.url;
        String model = cfg.model;
        int dim = cfg.dim;
        long timeoutMs = cfg.timeoutMs;
        String apiKey = cfg.apiKey;

        JsonObject body = new JsonObject();
        body.addProperty("model", model);
        JsonArray input = new JsonArray();
        for (String t : texts) {
            input.add(t == null ? "" : t);
        }
        body.add("input", input);
        if (dim > 0) {
            body.addProperty("dimensions", dim);
        }
        String bodyJson = body.toString();

        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(bodyJson));
        if (!Strings.isNullOrEmpty(apiKey)) {
            reqBuilder.header("Authorization", "Bearer " + apiKey);
        }
        HttpRequest req = reqBuilder.build();

        HttpResponse<String> resp = sendWithRetry(HTTP, req);
        if (resp.statusCode() != 200) {
            throw new ContextException(ContextErrorCode.VECTOR_NOT_READY,
                    "FE embedding HTTP " + resp.statusCode() + ": " + resp.body());
        }

        JsonElement parsed = JsonParser.parseString(resp.body());
        if (!parsed.isJsonObject()) {
            throw new ContextException(ContextErrorCode.VECTOR_NOT_READY,
                    "FE embedding response not a JSON object");
        }
        JsonObject root = parsed.getAsJsonObject();
        JsonElement dataEl = root.get("data");
        if (dataEl == null || !dataEl.isJsonArray()) {
            throw new ContextException(ContextErrorCode.VECTOR_NOT_READY,
                    "FE embedding response missing data array");
        }
        JsonArray data = dataEl.getAsJsonArray();
        if (data.size() != texts.size()) {
            throw new ContextException(ContextErrorCode.VECTOR_NOT_READY,
                    "FE embedding response size " + data.size() + " != " + texts.size());
        }

        float[][] vectors = new float[texts.size()][];
        for (int k = 0; k < data.size(); k++) {
            JsonObject entry = data.get(k).getAsJsonObject();
            int outIdx = k;
            JsonElement idxEl = entry.get("index");
            if (idxEl != null && idxEl.isJsonPrimitive() && idxEl.getAsJsonPrimitive().isNumber()) {
                outIdx = idxEl.getAsInt();
            }
            if (outIdx < 0 || outIdx >= texts.size()) {
                throw new ContextException(ContextErrorCode.VECTOR_NOT_READY,
                        "FE embedding response index out of range: " + outIdx);
            }
            JsonElement embEl = entry.get("embedding");
            if (embEl == null || !embEl.isJsonArray()) {
                throw new ContextException(ContextErrorCode.VECTOR_NOT_READY,
                        "FE embedding response missing embedding array at index " + outIdx);
            }
            JsonArray emb = embEl.getAsJsonArray();
            float[] vec = new float[emb.size()];
            for (int i = 0; i < emb.size(); i++) {
                vec[i] = emb.get(i).getAsFloat();
            }
            vectors[outIdx] = vec;
        }

        List<float[]> out = new ArrayList<>(vectors.length);
        for (float[] v : vectors) {
            if (v == null) {
                throw new ContextException(ContextErrorCode.VECTOR_NOT_READY,
                        "FE embedding response missing some indices");
            }
            out.add(v);
        }
        return out;
    }

    /**
     * Send {@code req} with a bounded retry on transient connection failures. Only {@link IOException}
     * (e.g. {@code SocketException: Connection reset} from a stale pooled connection) is retried — the
     * dead connection is evicted by the JDK client on failure, so the next attempt reconnects. A
     * non-2xx HTTP status is NOT an {@code IOException} and is left for the caller to classify as a
     * real provider error. A request/connect timeout ({@link HttpTimeoutException}, itself an
     * {@code IOException}) means the provider is genuinely slow or black-holing, so it fails fast
     * without retry — retrying would just multiply the wait by {@link #MAX_SEND_ATTEMPTS}.
     * {@code InterruptedException} restores the interrupt flag and fails fast.
     */
    static HttpResponse<String> sendWithRetry(HttpClient client, HttpRequest req) {
        IOException lastIo = null;
        for (int attempt = 1; attempt <= MAX_SEND_ATTEMPTS; attempt++) {
            try {
                return client.send(req, HttpResponse.BodyHandlers.ofString());
            } catch (HttpTimeoutException e) {
                // Covers HttpConnectTimeoutException too. A timeout is not a stale-connection blip;
                // retrying only stacks another full timeout, so fail fast.
                throw new ContextException(ContextErrorCode.VECTOR_NOT_READY,
                        "FE embedding provider timed out after " + e.getMessage());
            } catch (IOException e) {
                lastIo = e;
                LOG.warn("FeEmbed send attempt {}/{} failed: {}", attempt, MAX_SEND_ATTEMPTS, e.toString());
                if (attempt < MAX_SEND_ATTEMPTS) {
                    try {
                        Thread.sleep(RETRY_BACKOFF_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new ContextException(ContextErrorCode.VECTOR_NOT_READY,
                                "FE embedding interrupted during retry backoff");
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ContextException(ContextErrorCode.VECTOR_NOT_READY,
                        "FE embedding HTTP send interrupted: " + e.getMessage());
            }
        }
        throw new ContextException(ContextErrorCode.VECTOR_NOT_READY,
                "FE embedding HTTP send failed after " + MAX_SEND_ATTEMPTS + " attempts: " + lastIo.getMessage());
    }
}
