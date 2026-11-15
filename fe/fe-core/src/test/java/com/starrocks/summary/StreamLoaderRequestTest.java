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

import com.google.common.collect.Multimap;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.NodeSelector;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.UtFrameUtils;
import com.sun.net.httpserver.HttpServer;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

@Timeout(15)
public class StreamLoaderRequestTest {
    @BeforeAll
    public static void initialize() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @Test
    public void testQueryHistoryRequestPreservesColumnsPayloadAndUniqueLabels() throws Exception {
        List<String> columns = List.of("dt", "frontend", "db", "sql_digest", "sql", "plan", "plan_costs", "query_ms", "other");
        String payload = "[{\"sql\":\"SELECT '中文😀'\",\"query_ms\":1.5}]";
        List<String> receivedPayloads = new ArrayList<>();
        List<String> receivedLabels = new ArrayList<>();
        List<String> receivedColumns = new ArrayList<>();
        List<String> receivedPaths = new ArrayList<>();
        List<String> receivedAuth = new ArrayList<>();
        List<String> receivedFilterRatios = new ArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/", exchange -> {
            receivedPayloads.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            receivedLabels.add(exchange.getRequestHeaders().getFirst("label"));
            receivedColumns.add(exchange.getRequestHeaders().getFirst("columns"));
            receivedPaths.add(exchange.getRequestURI().getPath());
            receivedAuth.add(exchange.getRequestHeaders().getFirst("Authorization"));
            receivedFilterRatios.add(exchange.getRequestHeaders().getFirst("max_filter_ratio"));
            byte[] body = "{\"Status\":\"Success\",\"Message\":\"OK\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
        try {
            Backend be = new Backend(7L, "127.0.0.1", 9050);
            be.setHttpPort(server.getAddress().getPort());
            new MockUp<NodeSelector>() {
                @Mock
                public List<Long> seqChooseBackendIds(int count, boolean available, boolean create,
                                                      Multimap<String, String> location) {
                    return List.of(7L);
                }
            };
            new MockUp<SystemInfoService>() {
                @Mock
                public ComputeNode getBackendOrComputeNode(long id) {
                    return be;
                }
            };
            for (int i = 0; i < 2; i++) {
                StreamLoader loader = new StreamLoader("_statistics_", "query_history", columns);
                Assertions.assertEquals(200, loader.loadBatch("query_history", payload).status());
            }
            Assertions.assertEquals(List.of(payload, payload), receivedPayloads);
            Assertions.assertEquals(2, receivedLabels.stream().distinct().count());
            var fe = GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf();
            for (int i = 0; i < 2; i++) {
                Assertions.assertTrue(receivedLabels.get(i).startsWith("query_history" + fe.getHost().replace(".", "_")));
                FeNameFormat.checkLabel(receivedLabels.get(i));
                Assertions.assertEquals(String.join(",", columns), receivedColumns.get(i));
                Assertions.assertEquals("/api/_statistics_/query_history/_stream_load", receivedPaths.get(i));
                Assertions.assertEquals(fe.getHost() + ":" + fe.getNodeName(), new String(Base64.getDecoder()
                        .decode(receivedAuth.get(i).substring(6)), StandardCharsets.UTF_8));
                Assertions.assertNull(receivedFilterRatios.get(i), "query history must retain strict filtering");
            }
        } finally {
            server.stop(0);
        }
    }
}
