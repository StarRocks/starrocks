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

package com.starrocks.summary;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.http.HttpStatus;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

class StreamLoader {
    private static final String LOAD_URL_PATTERN = "/api/%s/%s/_stream_load";

    private static final int CONNECT_TIMEOUT_SECOND = 5;

    // This class is shared by two very different callers (AuditLoaderMgr: small batches, capped by
    // audit_loader_batch_max_bytes, default 50MB; QueryHistoryMgr: up to 200MB of plan-heavy rows),
    // so the request timeout and max_filter_ratio below are per-instance, not one-size-fits-all
    // class constants. Getting this wrong for QueryHistoryMgr is not academic: it discards its
    // whole in-memory batch on any failure with no retry (unlike AuditLoaderMgr's copy-then-remove
    // and bounded retry), so a timeout too short for its legitimately larger, slower batches would
    // turn "still loading" into permanent data loss.

    // Tuned for the builtin audit loader only (see AuditLoaderMgr's 2-arg constructor call and its
    // MAX_BATCH_FLUSH_RETRY): a short fixed bound so a BE that accepts the connection but never
    // answers cannot wedge that daemon thread forever, favoring fast self-heal over precisely
    // avoiding a duplicate write, since an undeliverable audit batch is eventually discarded
    // anyway. Audit batches are small, so 10s comfortably covers a merely slow-but-alive BE.
    private static final int AUDIT_REQUEST_TIMEOUT_SECOND = 10;

    // Unexpected data makes the stream load fail, and a failed batch is then retried every cycle
    // until MAX_BATCH_FLUSH_RETRY finally gives up on it. Those retries are not free: the batch sits
    // at the head of the queue the whole time while new events keep arriving behind it, so the
    // buffer backs up against its byte cap and FE memory pressure rises. Tolerating a small share of
    // rows ends that loop on the first attempt instead of paying for it in retries and heap.
    // 0.05 keeps that window deliberately narrow: a few rows may be given up, but not a meaningful
    // slice of the audit trail, and a larger fault still fails the batch instead of being absorbed.
    // Only applied for the audit loader; other callers keep the strict all-or-nothing default.
    private static final String AUDIT_MAX_FILTER_RATIO = "0.05";

    // Reuse a single HttpClient instance across all stream load batches.
    // java.net.http.HttpClient is thread-safe and designed to be used as a singleton.
    // Creating a new instance per batch leaks FDs (selector epoll/eventfd/pipe + idle
    // sockets) because the JDK 17 HttpClient has no close() method and relies on GC,
    // which fails to keep up under sustained periodic load.
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(CONNECT_TIMEOUT_SECOND))
            .version(HttpClient.Version.HTTP_1_1)
            .build();

    // Monotonic per-process sequence appended to every label, so a label stays unique even when one
    // flush() loop sends several batches within the same wall-clock second. It replaces
    // System.identityHashCode(this), which only kept labels apart as far as the JVM happened to hand
    // out distinct identity hashes to these short-lived instances; a repeat there is rejected as a
    // duplicate label and costs the caller a whole retry attempt.
    private static final AtomicLong LABEL_SEQ = new AtomicLong(0);

    private final String loadUrlStr;

    private final List<String> columns;

    private final int requestTimeoutSecond;

    // null omits the header entirely: strict, all-or-nothing semantics (the pre-existing default
    // for every caller other than the audit loader).
    private final String maxFilterRatio;

    /**
     * Load without an explicit column list, tuned for the builtin audit loader: a short fixed
     * request timeout and a small {@code max_filter_ratio} (see the constants above). The JSON
     * keys are mapped to the table columns by name, and a key without a matching column is
     * ignored instead of failing the batch, so a payload produced by a newer FE still loads into
     * a table that predates the new column.
     */
    public StreamLoader(String db, String tbl) {
        this(db, tbl, null, AUDIT_REQUEST_TIMEOUT_SECOND, AUDIT_MAX_FILTER_RATIO);
    }

    /**
     * Load with an explicit column list (e.g. QueryHistoryMgr). Uses
     * {@code Config.stream_load_default_timeout_second} as the request timeout, guarded against
     * that unvalidated config being set to {@code <= 0}, and no {@code max_filter_ratio}.
     */
    public StreamLoader(String db, String tbl, List<String> columns) {
        this(db, tbl, columns, safeStreamLoadTimeoutSecond(), null);
    }

    private StreamLoader(String db, String tbl, List<String> columns, int requestTimeoutSecond,
                          String maxFilterRatio) {
        this.columns = columns;
        this.loadUrlStr = String.format(LOAD_URL_PATTERN, db, tbl);
        this.requestTimeoutSecond = requestTimeoutSecond;
        this.maxFilterRatio = maxFilterRatio;
    }

    private static int safeStreamLoadTimeoutSecond() {
        int configured = Config.stream_load_default_timeout_second;
        return configured > 0 ? configured : 600;
    }

    public record Response(int status, String msg) {}

    public Response loadBatch(String label, String sb) throws URISyntaxException, IOException,
            InterruptedException {
        Frontend fe = GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf();
        label += fe.getHost().replace(".", "_");
        label += "_" + LABEL_SEQ.getAndIncrement();
        label += "_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));

        String authString = fe.getHost() + ":" + fe.getNodeName();
        String authEncoding = Base64.getEncoder().encodeToString(authString.getBytes());

        Optional<ComputeNode> be = chooseBENode();
        if (be.isEmpty()) {
            return new Response(HttpStatus.SC_PRECONDITION_FAILED, "doesn't found available be node");
        }
        URI uri = new URI("http", null, be.get().getHost(), be.get().getHttpPort(), loadUrlStr, null, null);
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .header("Authorization", "Basic " + authEncoding)
                .header("Content-Type", "text/plain; charset=UTF-8")
                .header("format", "json")
                .header("label", label)
                .header("strip_outer_array", "true");
        if (maxFilterRatio != null) {
            builder.header("max_filter_ratio", maxFilterRatio);
        }
        if (columns != null) {
            builder.header("columns", String.join(",", columns));
        }
        builder.timeout(Duration.ofSeconds(requestTimeoutSecond));
        HttpRequest request = builder.PUT(HttpRequest.BodyPublishers.ofString(sb)).build();

        HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == HttpStatus.SC_OK) {
            JsonElement obj = JsonParser.parseString(response.body());
            String status = obj.getAsJsonObject().get("Status").getAsString();
            String message = obj.getAsJsonObject().get("Message").getAsString();

            if (!status.equalsIgnoreCase("success")) {
                return new Response(HttpStatus.SC_INTERNAL_SERVER_ERROR, message);
            } else {
                return new Response(HttpStatus.SC_OK, message);
            }
        }
        return new Response(response.statusCode(), response.body());
    }

    private static Optional<ComputeNode> chooseBENode() {
        // Choose a backend sequentially, or choose a cn in shared_data mode
        List<Long> nodeIds = new ArrayList<>();
        if (RunMode.isSharedDataMode()) {
            List<Long> computeIds = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                    .getAllComputeNodeIds(WarehouseManager.DEFAULT_RESOURCE);
            for (long nodeId : computeIds) {
                ComputeNode node =
                        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
                if (node != null && node.isAvailable()) {
                    nodeIds.add(nodeId);
                }
            }
            Collections.shuffle(nodeIds);
        } else {
            SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            nodeIds = systemInfoService.getNodeSelector().seqChooseBackendIds(1, true, false, null);
        }
        if (CollectionUtils.isEmpty(nodeIds)) {
            return Optional.empty();
        }
        ComputeNode node =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeIds.get(0));
        if (node == null) {
            return Optional.empty();
        }
        return Optional.of(node);
    }
}
