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

class StreamLoader {
    private static final String LOAD_URL_PATTERN = "/api/%s/%s/_stream_load";

    private static final int CONNECT_TIMEOUT_SECOND = 5;
    private static final int REQUEST_TIMEOUT_SECOND = 60;

    // Reuse a single HttpClient instance across all stream load batches.
    // java.net.http.HttpClient is thread-safe and designed to be used as a singleton.
    // Creating a new instance per batch leaks FDs (selector epoll/eventfd/pipe + idle
    // sockets) because the JDK 17 HttpClient has no close() method and relies on GC,
    // which fails to keep up under sustained periodic load.
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(CONNECT_TIMEOUT_SECOND))
            .version(HttpClient.Version.HTTP_1_1)
            .build();

    private final String loadUrlStr;

    private final List<String> columns;

    public StreamLoader(String db, String tbl, List<String> columns) {
        this.columns = columns;
        this.loadUrlStr = String.format(LOAD_URL_PATTERN, db, tbl);
    }

    public record Response(int status, String msg) {}

    public Response loadBatch(String label, String sb) throws URISyntaxException, IOException,
            InterruptedException {
        Frontend fe = GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf();
        label += fe.getHost().replace(".", "_");
        label += "_" + System.identityHashCode(this);
        label += "_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));

        String authString = fe.getHost() + ":" + fe.getNodeName();
        String authEncoding = Base64.getEncoder().encodeToString(authString.getBytes());

        Optional<ComputeNode> be = chooseBENode();
        if (be.isEmpty()) {
            return new Response(HttpStatus.SC_PRECONDITION_FAILED, "doesn't found available be node");
        }
        URI uri = new URI("http", null, be.get().getHost(), be.get().getHttpPort(), loadUrlStr, null, null);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(REQUEST_TIMEOUT_SECOND))
                .header("Authorization", "Basic " + authEncoding)
                .header("Content-Type", "text/plain; charset=UTF-8")
                .header("format", "json")
                .header("label", label)
                .header("columns", String.join(",", columns))
                .header("strip_outer_array", "true")
                .PUT(HttpRequest.BodyPublishers.ofString(sb))
                .build();

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
