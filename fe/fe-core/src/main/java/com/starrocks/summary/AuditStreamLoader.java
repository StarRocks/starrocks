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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import org.apache.http.HttpStatus;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Audit-only response handling; query history continues to use the shared StreamLoader. */
class AuditStreamLoader {
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).version(HttpClient.Version.HTTP_1_1).build();
    private final String path;

    enum Outcome { SUCCESS, COMMITTED_PENDING_VISIBILITY, ALREADY_COMMITTED, IN_PROGRESS, UNKNOWN, FAILED }

    // Missing/null counts are unknown. Malformed supplied counts invalidate the statistics group,
    // so an otherwise plausible filtered count cannot silently survive a corrupt response.
    record LoadStats(Long totalRows, Long loadedRows, Long filteredRows, String errorUrl, boolean invalidCounts) {
        LoadStats(Long totalRows, Long loadedRows, Long filteredRows, String errorUrl) {
            this(totalRows, loadedRows, filteredRows, errorUrl, false);
        }
    }

    record Response(int status, String msg, LoadStats stats, Outcome outcome) {
        Response(int status, String msg, LoadStats stats) {
            this(status, msg, stats, status == HttpStatus.SC_OK ? Outcome.SUCCESS : Outcome.FAILED);
        }

        Response(int status, String msg) {
            this(status, msg, null);
        }

        boolean committed() {
            return outcome == Outcome.SUCCESS || outcome == Outcome.COMMITTED_PENDING_VISIBILITY
                    || outcome == Outcome.ALREADY_COMMITTED;
        }
    }

    AuditStreamLoader(String db, String table) {
        path = String.format("/api/%s/%s/_stream_load", db, table);
    }

    static String newLabel() {
        Frontend fe = GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf();
        // Allocate once per batch. The UUID prevents collisions with another FE process/startup.
        // Retain the host representation; FQDN/IPv6 identity support is a separate follow-up.
        return "audit_loader" + fe.getHost().replace(".", "_") + "_" + UUID.randomUUID();
    }

    public Response loadBatch(String label, String payload) throws URISyntaxException, IOException, InterruptedException {
        Frontend fe = GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf();
        String auth = Base64.getEncoder().encodeToString(
                (fe.getHost() + ":" + fe.getNodeName()).getBytes(StandardCharsets.UTF_8));
        Optional<ComputeNode> be = chooseBENode();
        if (be.isEmpty()) {
            return new Response(HttpStatus.SC_PRECONDITION_FAILED, "no available BE node");
        }
        URI uri = new URI("http", null, be.get().getHost(), be.get().getHttpPort(), path, null, null);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .header("Authorization", "Basic " + auth)
                .header("Content-Type", "text/plain; charset=UTF-8")
                .header("format", "json")
                .header("label", label)
                .header("strip_outer_array", "true")
                .header("max_filter_ratio", "0.05")
                // A duplicate label can be rejected before its (potentially large) body is sent.
                .expectContinue(true)
                .timeout(Duration.ofSeconds(10))
                .PUT(HttpRequest.BodyPublishers.ofString(payload)).build();
        // Reuse the bounded body-read/cancellation implementation without changing shared callers.
        HttpResponse<String> response = StreamLoader.sendRequest(HTTP_CLIENT, request);
        return parseResponse(response.statusCode(), response.body(), label);
    }

    @VisibleForTesting
    static Response parseResponse(int httpStatus, String body) {
        return parseResponse(httpStatus, body, null);
    }

    @VisibleForTesting
    static Response parseResponse(int httpStatus, String body, String expectedLabel) {
        if (httpStatus != HttpStatus.SC_OK) {
            return new Response(httpStatus, body, null, Outcome.UNKNOWN);
        }
        JsonObject obj = JsonParser.parseString(body).getAsJsonObject();
        String status = stringValue(obj, "Status");
        String message = stringValue(obj, "Message");
        if ("Label Already Exists".equalsIgnoreCase(status) && expectedLabel != null
                && expectedLabel.equals(stringValue(obj, "Label"))) {
            String existingStatus = stringValue(obj, "ExistingJobStatus");
            if ("FINISHED".equalsIgnoreCase(existingStatus)) {
                // FE maps both COMMITTED and VISIBLE to FINISHED. These counters describe the
                // rejected duplicate request, not the original load; its filtering is unknown.
                return new Response(httpStatus, message, null, Outcome.ALREADY_COMMITTED);
            }
            if ("RUNNING".equalsIgnoreCase(existingStatus)) {
                return new Response(httpStatus, message, null, Outcome.IN_PROGRESS);
            }
        }
        Outcome outcome;
        if ("Success".equalsIgnoreCase(status)) {
            outcome = Outcome.SUCCESS;
        } else if ("Publish Timeout".equalsIgnoreCase(status)) {
            // Commit succeeded; re-submitting with a new label could duplicate the entire batch.
            outcome = Outcome.COMMITTED_PENDING_VISIBILITY;
        } else {
            return new Response(HttpStatus.SC_INTERNAL_SERVER_ERROR,
                    "status=" + status + ", message=" + message, null,
                    "Fail".equalsIgnoreCase(status) ? Outcome.FAILED : Outcome.UNKNOWN);
        }
        RowCount total = readRowCount(obj, "NumberTotalRows");
        RowCount loaded = readRowCount(obj, "NumberLoadedRows");
        RowCount filtered = readRowCount(obj, "NumberFilteredRows");
        return new Response(httpStatus, message, new LoadStats(total.value(), loaded.value(), filtered.value(),
                stringValue(obj, "ErrorURL"), total.invalid() || loaded.invalid() || filtered.invalid()), outcome);
    }

    private record RowCount(Long value, boolean invalid) { }

    private static RowCount readRowCount(JsonObject obj, String name) {
        JsonElement value = obj.get(name);
        if (value == null || value.isJsonNull()) {
            return new RowCount(null, false);
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
            return new RowCount(null, true);
        }
        try {
            return new RowCount(value.getAsBigDecimal().longValueExact(), false);
        } catch (NumberFormatException | ArithmeticException e) {
            return new RowCount(null, true);
        }
    }

    private static String stringValue(JsonObject obj, String name) {
        JsonElement value = obj.get(name);
        return value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isString()
                ? value.getAsString() : null;
    }

    private static Optional<ComputeNode> chooseBENode() {
        // Preserve the shared loader's BE/CN selection, including the default warehouse in shared_data.
        List<Long> nodeIds = new ArrayList<>();
        var state = GlobalStateMgr.getCurrentState();
        var clusterInfo = state.getNodeMgr().getClusterInfo();
        if (RunMode.isSharedDataMode()) {
            for (long nodeId : state.getWarehouseMgr().getAllComputeNodeIds(WarehouseManager.DEFAULT_RESOURCE)) {
                ComputeNode node = clusterInfo.getBackendOrComputeNode(nodeId);
                if (node != null && node.isAvailable()) {
                    nodeIds.add(nodeId);
                }
            }
            Collections.shuffle(nodeIds);
        } else {
            nodeIds = clusterInfo.getNodeSelector().seqChooseBackendIds(1, true, false, null);
        }
        if (nodeIds == null || nodeIds.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(clusterInfo.getBackendOrComputeNode(nodeIds.get(0)));
    }
}
