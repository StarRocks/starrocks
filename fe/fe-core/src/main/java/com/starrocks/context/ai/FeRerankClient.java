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

package com.starrocks.context.ai;

import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;
import com.starrocks.server.AIProviderMgr;
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
import java.util.Comparator;
import java.util.List;

/**
 * FE-side cross-encoder rerank caller. Posts one HTTP request to a rerank provider in the de-facto
 * standard <b>Cohere Rerank v2</b> shape — request {@code {model, query, documents:[...], top_n}},
 * response {@code {results:[{index, relevance_score}]}} — which Jina, Voyage, OpenRouter
 * ({@code /api/v1/rerank}) and local TEI/vLLM rerank servers all share. Returns the input document
 * indices reordered by descending relevance.
 *
 * <p>Provider settings (endpoint / model / api_key / timeout_ms / deadline_ms / max_documents) come
 * from an {@link AIProvider} of type {@link AIProviderType#RERANK}. This client <b>throws</b> on any
 * failure (HTTP non-2xx, network error, malformed response); the second-phase rerank in
 * {@code ContextSearchExecutor} catches it and falls back to the first-stage fusion order, so a
 * flaky/misconfigured reranker degrades rather than breaks search.
 *
 * <p><b>Latency is bounded.</b> The whole call (all retry attempts plus backoff) is capped by an
 * overall {@code deadline_ms} budget (default {@value #DEFAULT_OVERALL_DEADLINE_MS}ms), and a timeout
 * is never retried — only a fast-failing connection error (refused/reset/unreachable) or an HTTP 5xx
 * is retried. So a slow or black-holing reranker degrades within the deadline instead of stalling the
 * synchronous search for {@code timeout_ms × attempts}.
 */
public final class FeRerankClient {

    private static final Logger LOG = LogManager.getLogger(FeRerankClient.class);

    static final int MAX_SEND_ATTEMPTS = 3;
    static final long RETRY_BACKOFF_MS = 200;
    // Cohere recommends <= 1000 documents per request; used as the default cap when the provider
    // does not set max_documents.
    static final int DEFAULT_MAX_DOCUMENTS = 1000;
    // Per-request response timeout when the provider does not set timeout_ms.
    static final long DEFAULT_TIMEOUT_MS = 30000;
    // Overall wall-clock budget for the whole rerank call (all attempts + backoff) when the provider
    // does not set deadline_ms. Conservative so a slow/black-holing reranker cannot stall a
    // synchronous search for long before the caller degrades to fusion order.
    static final long DEFAULT_OVERALL_DEADLINE_MS = 10000;
    // Connection-establishment timeout. Kept well under the overall deadline so a black-holing host
    // (one that drops SYN) is detected fast instead of eating the whole budget.
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(3);

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(CONNECT_TIMEOUT)
            .build();

    private FeRerankClient() {
    }

    /** One reranked hit: an index into the original {@code documents} list plus its relevance score. */
    public static final class ScoredIndex {
        public final int index;
        public final double score;

        public ScoredIndex(int index, double score) {
            this.index = index;
            this.score = score;
        }
    }

    /**
     * Resolve a rerank provider by name, or the default rerank provider when {@code name} is empty.
     * Throws if it does not exist or is not a rerank-type provider.
     */
    public static AIProvider resolveProvider(String name) {
        AIProviderMgr mgr = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        AIProvider provider;
        if (Strings.isNullOrEmpty(name)) {
            provider = mgr.getDefaultProvider(AIProviderType.RERANK);
            if (provider == null) {
                throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                        "no DEFAULT RERANK provider configured",
                        "Run: CREATE AI PROVIDER ... TYPE rerank ...; SET <name> AS DEFAULT AI PROVIDER");
            }
        } else {
            provider = mgr.getProvider(name);
            if (provider == null) {
                throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                        "rerank provider '" + name + "' does not exist");
            }
            if (provider.getType() != AIProviderType.RERANK) {
                throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                        "AI provider '" + name + "' is type " + provider.getType().lower() + ", not rerank");
            }
        }
        return provider;
    }

    /**
     * Rerank {@code documents} against {@code query}. Returns {@link ScoredIndex} entries (index into
     * the input list + relevance score), sorted by descending relevance. If the provider caps
     * documents, only the first N are sent and only those indices are returned.
     */
    public static List<ScoredIndex> rerank(AIProvider provider, String query, List<String> documents) {
        if (documents == null || documents.isEmpty()) {
            return new ArrayList<>();
        }
        String url = provider.getEndpoint();
        if (Strings.isNullOrEmpty(url)) {
            throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                    "RERANK provider '" + provider.getName() + "' has no endpoint set");
        }
        Integer maxDocsObj = provider.getMaxDocuments();
        int maxDocs = maxDocsObj != null && maxDocsObj > 0 ? maxDocsObj : DEFAULT_MAX_DOCUMENTS;
        List<String> docs = documents.size() > maxDocs ? documents.subList(0, maxDocs) : documents;
        Integer timeoutObj = provider.getTimeoutMs();
        long timeoutMs = timeoutObj != null && timeoutObj > 0 ? timeoutObj : DEFAULT_TIMEOUT_MS;
        Integer deadlineObj = provider.getDeadlineMs();
        long deadlineMs = deadlineObj != null && deadlineObj > 0 ? deadlineObj : DEFAULT_OVERALL_DEADLINE_MS;
        String apiKey = provider.getApiKey();

        JsonObject body = new JsonObject();
        body.addProperty("model", provider.getModel());
        body.addProperty("query", query == null ? "" : query);
        JsonArray docArray = new JsonArray();
        for (String d : docs) {
            docArray.add(d == null ? "" : d);
        }
        body.add("documents", docArray);
        body.addProperty("top_n", docs.size());

        // sendWithDeadline only returns a 2xx response; non-2xx and failures throw.
        HttpResponse<String> resp = sendWithDeadline(url, apiKey, body.toString(), timeoutMs, deadlineMs);

        JsonElement parsed = JsonParser.parseString(resp.body());
        if (!parsed.isJsonObject()) {
            throw new ContextException(ContextErrorCode.INTERNAL_ERROR, "FE rerank response not a JSON object");
        }
        JsonElement resultsEl = parsed.getAsJsonObject().get("results");
        if (resultsEl == null || !resultsEl.isJsonArray()) {
            throw new ContextException(ContextErrorCode.INTERNAL_ERROR, "FE rerank response missing results array");
        }
        JsonArray results = resultsEl.getAsJsonArray();
        List<ScoredIndex> out = new ArrayList<>(results.size());
        for (JsonElement el : results) {
            JsonObject entry = el.getAsJsonObject();
            JsonElement idxEl = entry.get("index");
            JsonElement scoreEl = entry.get("relevance_score");
            if (idxEl == null || scoreEl == null) {
                throw new ContextException(ContextErrorCode.INTERNAL_ERROR,
                        "FE rerank result missing index/relevance_score");
            }
            int idx = idxEl.getAsInt();
            if (idx < 0 || idx >= docs.size()) {
                throw new ContextException(ContextErrorCode.INTERNAL_ERROR,
                        "FE rerank result index out of range: " + idx);
            }
            out.add(new ScoredIndex(idx, scoreEl.getAsDouble()));
        }
        out.sort(Comparator.comparingDouble((ScoredIndex s) -> s.score).reversed());
        return out;
    }

    /**
     * POST the rerank request with a bounded retry policy under an overall wall-clock deadline.
     * Returns the first 2xx response; throws (so the caller degrades) on any terminal outcome.
     *
     * <p><b>Retry policy</b> — only fast-failing transient errors are retried:
     * <ul>
     *   <li>a genuine connection failure (refused / reset / unreachable), and</li>
     *   <li>an HTTP 5xx (server overload / mid-deploy).</li>
     * </ul>
     * A <b>timeout</b> (request or connect) is <b>not</b> retried — retrying a slow/black-holing
     * endpoint only multiplies the stall, so we degrade immediately. A 4xx is a client/config error
     * and also fails fast. Every attempt, and the backoff between attempts, is clamped to the
     * remaining {@code overallDeadlineMs} budget; each request's response timeout is
     * {@code min(perRequestTimeoutMs, remaining budget)}.
     */
    static HttpResponse<String> sendWithDeadline(String url, String apiKey, String bodyJson,
            long perRequestTimeoutMs, long overallDeadlineMs) {
        long deadlineNanos = System.nanoTime() + overallDeadlineMs * 1_000_000L;
        IOException lastConnectIo = null;
        int lastStatus = -1;
        String lastBody = null;
        for (int attempt = 1; attempt <= MAX_SEND_ATTEMPTS; attempt++) {
            long remainingMs = (deadlineNanos - System.nanoTime()) / 1_000_000L;
            if (remainingMs <= 0) {
                throw new ContextException(ContextErrorCode.INTERNAL_ERROR,
                        "FE rerank exceeded overall deadline of " + overallDeadlineMs + "ms");
            }
            long attemptTimeoutMs = Math.min(perRequestTimeoutMs, remainingMs);
            HttpRequest req = buildRequest(url, apiKey, bodyJson, attemptTimeoutMs);
            try {
                HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
                int status = resp.statusCode();
                if (status / 100 == 2) {
                    return resp;
                }
                lastStatus = status;
                lastBody = resp.body();
                // 5xx is transient -> retry; everything else (4xx) is terminal -> fail fast.
                if (status < 500 || status >= 600) {
                    throw new ContextException(ContextErrorCode.INTERNAL_ERROR,
                            "FE rerank HTTP " + status + ": " + resp.body());
                }
                LOG.warn("FeRerank attempt {}/{} got HTTP {}", attempt, MAX_SEND_ATTEMPTS, status);
            } catch (HttpTimeoutException e) {
                // Request or connect timeout: not retried — degrade now so search isn't stalled.
                throw new ContextException(ContextErrorCode.INTERNAL_ERROR,
                        "FE rerank timed out after " + attemptTimeoutMs + "ms: " + e.getMessage());
            } catch (IOException e) {
                // Genuine connection failure (refused / reset / unreachable) — fast, worth a retry.
                lastConnectIo = e;
                LOG.warn("FeRerank attempt {}/{} connect failed: {}", attempt, MAX_SEND_ATTEMPTS, e.toString());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ContextException(ContextErrorCode.INTERNAL_ERROR,
                        "FE rerank HTTP send interrupted: " + e.getMessage());
            }
            // Backoff before the next attempt, but never past the deadline.
            if (attempt < MAX_SEND_ATTEMPTS) {
                long backoffMs = Math.min(RETRY_BACKOFF_MS, (deadlineNanos - System.nanoTime()) / 1_000_000L);
                if (backoffMs > 0) {
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new ContextException(ContextErrorCode.INTERNAL_ERROR,
                                "FE rerank interrupted during retry backoff");
                    }
                }
            }
        }
        if (lastStatus >= 500) {
            throw new ContextException(ContextErrorCode.INTERNAL_ERROR,
                    "FE rerank HTTP " + lastStatus + " after " + MAX_SEND_ATTEMPTS + " attempts: " + lastBody);
        }
        throw new ContextException(ContextErrorCode.INTERNAL_ERROR,
                "FE rerank connect failed after " + MAX_SEND_ATTEMPTS + " attempts: "
                        + (lastConnectIo == null ? "unknown error" : lastConnectIo.getMessage()));
    }

    private static HttpRequest buildRequest(String url, String apiKey, String bodyJson, long timeoutMs) {
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(bodyJson));
        if (!Strings.isNullOrEmpty(apiKey)) {
            b.header("Authorization", "Bearer " + apiKey);
        }
        return b.build();
    }
}
