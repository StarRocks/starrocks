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

package com.starrocks.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Requests pipelined on one connection, against the running server: a response that is written over time from the
 * executor, as HttpResultSender writes query results, is followed by the responses to the requests sent behind it,
 * in order and each whole, whether those run on the event loop or on the executor.
 */
public class HttpPipeliningTest extends StarRocksHttpTestCase {
    private static final String STREAM_URI = "/test/pipelining/stream";
    private static final String SYNC_URI = "/test/pipelining/sync";
    private static final int CHUNKS = 20;
    // Whether each run of the actions below was on the connection's event loop.
    private static final List<Boolean> STREAM_ON_EVENT_LOOP = new CopyOnWriteArrayList<>();
    private static final List<Boolean> SYNC_ON_EVENT_LOOP = new CopyOnWriteArrayList<>();

    // Writes its response a chunk at a time from the executor thread, pausing between chunks.
    private static class SlowStreamAction extends BaseAction {
        SlowStreamAction(ActionController controller) {
            super(controller);
        }

        @Override
        public boolean supportAsyncHandler() {
            return true;
        }

        @Override
        public void execute(BaseRequest request, BaseResponse response) {
            ChannelHandlerContext ctx = request.getContext();
            STREAM_ON_EVENT_LOOP.add(ctx.executor().inEventLoop());
            HttpResponse header = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            HttpUtil.setTransferEncodingChunked(header, true);
            ctx.write(header);
            for (int i = 0; i < CHUNKS; i++) {
                ctx.writeAndFlush(new DefaultHttpContent(Unpooled.copiedBuffer("chunk-" + i + "\n",
                        StandardCharsets.UTF_8)));
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        }
    }

    // Answers at once, on the event loop.
    private static class SyncAction extends BaseAction {
        SyncAction(ActionController controller) {
            super(controller);
        }

        @Override
        public boolean supportAsyncHandler() {
            return false;
        }

        @Override
        public void execute(BaseRequest request, BaseResponse response) {
            SYNC_ON_EVENT_LOOP.add(request.getContext().executor().inEventLoop());
            response.appendContent("sync");
            writeResponse(request, response, HttpResponseStatus.OK);
        }
    }

    private static final class Received {
        final int status;
        final String body;

        Received(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }

    @BeforeAll
    public static void registerStreamAction() throws Exception {
        // The server is set up once per test class, so the action is registered once.
        httpController().registerHandler(HttpMethod.GET, STREAM_URI, new SlowStreamAction(httpController()));
        httpController().registerHandler(HttpMethod.GET, SYNC_URI, new SyncAction(httpController()));
    }

    private static String expectedStream() {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < CHUNKS; i++) {
            body.append("chunk-").append(i).append('\n');
        }
        return body.toString();
    }

    private String get(String uri) {
        return "GET " + uri + " HTTP/1.1\r\nHost: localhost\r\nAuthorization: " + rootAuth + "\r\n\r\n";
    }

    // Reads one response off the stream, with a body framed by Content-Length or by chunked encoding.
    private static Received readResponse(InputStream in) throws IOException {
        String statusLine = readLine(in);
        Assertions.assertTrue(statusLine.startsWith("HTTP/1.1 "), statusLine);
        int status = Integer.parseInt(statusLine.substring(9, 12));
        int contentLength = -1;
        boolean chunked = false;
        for (String header = readLine(in); !header.isEmpty(); header = readLine(in)) {
            String lower = header.toLowerCase(Locale.ROOT);
            if (lower.startsWith("content-length:")) {
                contentLength = Integer.parseInt(lower.substring("content-length:".length()).trim());
            } else if (lower.equals("transfer-encoding: chunked")) {
                chunked = true;
            }
        }
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        if (chunked) {
            for (int size = Integer.parseInt(readLine(in), 16); size > 0; size = Integer.parseInt(readLine(in), 16)) {
                body.write(readBytes(in, size));
                Assertions.assertEquals("", readLine(in));
            }
            Assertions.assertEquals("", readLine(in));
        } else {
            Assertions.assertTrue(contentLength >= 0, "neither Content-Length nor chunked: " + statusLine);
            body.write(readBytes(in, contentLength));
        }
        return new Received(status, body.toString(StandardCharsets.UTF_8));
    }

    private static byte[] readBytes(InputStream in, int size) throws IOException {
        byte[] bytes = in.readNBytes(size);
        Assertions.assertEquals(size, bytes.length, "connection closed in the middle of a response");
        return bytes;
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder line = new StringBuilder();
        for (int c = in.read(); c != '\n'; c = in.read()) {
            if (c < 0) {
                throw new EOFException("connection closed in the middle of a response");
            }
            if (c != '\r') {
                line.append((char) c);
            }
        }
        return line.toString();
    }

    @Test
    public void testResponsesFollowAStreamedOneInOrder() throws IOException {
        // Sent before any response is read: a streamed response, then a request handled on the event loop, one
        // that maps to no action (handled on the executor), another streamed one, and two more on the event loop.
        List<String> uris = List.of(STREAM_URI, SYNC_URI, "/no/such/action", STREAM_URI, "/api/health", SYNC_URI);
        try (Socket socket = new Socket("localhost", HTTP_PORT)) {
            socket.setSoTimeout(20_000);
            OutputStream out = socket.getOutputStream();
            StringBuilder requests = new StringBuilder();
            for (String uri : uris) {
                requests.append(get(uri));
            }
            out.write(requests.toString().getBytes(StandardCharsets.US_ASCII));
            out.flush();

            InputStream in = new BufferedInputStream(socket.getInputStream());
            Received first = readResponse(in);
            Assertions.assertEquals(200, first.status);
            Assertions.assertEquals(expectedStream(), first.body);

            Received sync = readResponse(in);
            Assertions.assertEquals(200, sync.status);
            Assertions.assertEquals("sync", sync.body);

            Assertions.assertEquals(404, readResponse(in).status);

            Received second = readResponse(in);
            Assertions.assertEquals(200, second.status);
            Assertions.assertEquals(expectedStream(), second.body);

            Received health = readResponse(in);
            Assertions.assertEquals(200, health.status);
            Assertions.assertTrue(health.body.contains("online_backend_num"), health.body);

            Assertions.assertEquals("sync", readResponse(in).body);

            // Nothing follows the last response.
            socket.setSoTimeout(500);
            Assertions.assertThrows(SocketTimeoutException.class, in::read);
        }
        // A request that waited still runs where its action asks to: on the event loop, or on the executor.
        Assertions.assertEquals(List.of(false, false), STREAM_ON_EVENT_LOOP);
        Assertions.assertEquals(List.of(true, true), SYNC_ON_EVENT_LOOP);
    }
}
