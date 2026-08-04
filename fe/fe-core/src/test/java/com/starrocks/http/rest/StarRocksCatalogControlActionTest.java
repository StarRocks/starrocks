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

package com.starrocks.http.rest;

import com.starrocks.connector.starrocks.StarRocksRemoteScanWire;
import com.starrocks.http.BaseRequest;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

/**
 * Tests for the request-decoding layer of the control-plane action: a malformed body is the
 * caller's error (status=400), and the single-hop forwarding flag must be read exactly as written
 * — an FE that misreads it either never forwards or forwards in a loop.
 */
public class StarRocksCatalogControlActionTest {

    private static BaseRequest postRequest(String uri, String body) {
        DefaultFullHttpRequest raw = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri,
                Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
        return new BaseRequest(null, raw);
    }

    @Test
    public void testParseBodyAcceptsWellFormedJson() throws Exception {
        BaseRequest request = postRequest("/api/_starrocks_remote/start_scan",
                "{\"session_id\":\"abc\"}");
        StarRocksRemoteScanWire.ScanControlRequest parsed =
                StarRocksCatalogControlAction.parseBody(request, StarRocksRemoteScanWire.ScanControlRequest.class);
        Assertions.assertEquals("abc", parsed.sessionId);
    }

    @Test
    public void testParseBodyRejectsMalformedJson() {
        BaseRequest request = postRequest("/api/_starrocks_remote/start_scan", "{not json");
        Assertions.assertThrows(StarRocksCatalogControlAction.MalformedRequestBodyException.class,
                () -> StarRocksCatalogControlAction.parseBody(
                        request, StarRocksRemoteScanWire.ScanControlRequest.class));
    }

    /** An empty body deserializes to null, which must be reported rather than passed downstream. */
    @Test
    public void testParseBodyRejectsEmptyBody() {
        BaseRequest request = postRequest("/api/_starrocks_remote/start_scan", "");
        Assertions.assertThrows(StarRocksCatalogControlAction.MalformedRequestBodyException.class,
                () -> StarRocksCatalogControlAction.parseBody(
                        request, StarRocksRemoteScanWire.ScanControlRequest.class));
    }

    @Test
    public void testParseBodyReadsPrepareScanFields() throws Exception {
        BaseRequest request = postRequest("/api/_starrocks_remote/prepare_scan",
                "{\"db\":\"db1\",\"table\":\"tbl1\",\"soft_limit\":5}");
        StarRocksRemoteScanWire.PrepareScanRequest parsed =
                StarRocksCatalogControlAction.parseBody(request, StarRocksRemoteScanWire.PrepareScanRequest.class);
        Assertions.assertEquals("db1", parsed.db);
        Assertions.assertEquals("tbl1", parsed.table);
        Assertions.assertEquals(5, parsed.softLimit);
    }

    /**
     * The calling cluster sends forward_request=true; a forwarding FE sends false so a lookup is a
     * single hop. Absent means false, so a peer that omits it cannot start a forwarding loop.
     */
    @Test
    public void testIsForwardRequestReadsTheFlag() {
        Assertions.assertTrue(StarRocksCatalogControlAction.isForwardRequest(
                postRequest("/api/_starrocks_remote/start_scan?forward_request=true", "{}")));
        Assertions.assertTrue(StarRocksCatalogControlAction.isForwardRequest(
                postRequest("/api/_starrocks_remote/start_scan?forward_request=TRUE", "{}")));
        Assertions.assertFalse(StarRocksCatalogControlAction.isForwardRequest(
                postRequest("/api/_starrocks_remote/start_scan?forward_request=false", "{}")));
        Assertions.assertFalse(StarRocksCatalogControlAction.isForwardRequest(
                postRequest("/api/_starrocks_remote/start_scan", "{}")));
        // Anything unparseable is not a forward request, which is the safe default.
        Assertions.assertFalse(StarRocksCatalogControlAction.isForwardRequest(
                postRequest("/api/_starrocks_remote/start_scan?forward_request=yes", "{}")));
    }
}
