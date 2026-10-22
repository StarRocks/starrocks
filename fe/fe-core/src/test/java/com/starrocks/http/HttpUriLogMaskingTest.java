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

import com.starrocks.http.action.StaticResourceAction;
import com.starrocks.http.rest.RestBaseAction;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The request uri logged by the HTTP layer keeps the value of a sanitized parameter (token) out of the log, also
// when the request is rejected or the uri cannot be parsed or decoded.
public class HttpUriLogMaskingTest extends StarRocksHttpTestCase {

    // Collects the messages logged to the loggers it is added to, and the exceptions logged with them.
    private static class MessageCollector extends AbstractAppender {
        private final List<String> messages = new CopyOnWriteArrayList<>();

        MessageCollector() {
            super("http-uri-log-masking-test-message-collector", null, null);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
            if (event.getThrown() != null) {
                messages.add("thrown: " + event.getThrown());
            }
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    // Captures, at DEBUG, what the HTTP server, the base actions and StaticResourceAction log while running body.
    private static List<String> captureLogs(ThrowingRunnable body) throws Exception {
        MessageCollector collector = new MessageCollector();
        List<Logger> loggers = List.of(
                (Logger) LogManager.getLogger(HttpServerHandler.class),
                (Logger) LogManager.getLogger(BaseAction.class),
                (Logger) LogManager.getLogger(RestBaseAction.class),
                (Logger) LogManager.getLogger(StaticResourceAction.class));
        List<Level> levels = loggers.stream().map(Logger::getLevel).collect(Collectors.toList());
        collector.start();
        // Add every appender before setting the levels: adding the first appender to a logger reconfigures the
        // loggers, which resets a level set before it.
        for (Logger logger : loggers) {
            logger.addAppender(collector);
        }
        for (Logger logger : loggers) {
            logger.setLevel(Level.DEBUG);
        }
        try {
            body.run();
        } finally {
            for (int i = 0; i < loggers.size(); i++) {
                loggers.get(i).setLevel(levels.get(i));
                loggers.get(i).removeAppender(collector);
            }
            collector.stop();
        }
        return collector.messages;
    }

    // Sends request as is through a raw socket, and returns the whole response.
    private static String sendRawRequest(String request) throws Exception {
        try (Socket socket = new Socket("localhost", HTTP_PORT)) {
            socket.getOutputStream().write(request.getBytes(StandardCharsets.UTF_8));
            return new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    // /api/show_proc requires auth, so a request without auth information is rejected and logged.
    private static final String URL = "/api/show_proc?path=%2F&token=TopSecret";

    private static void assertNoCredential(String text) {
        assertFalse(text.contains("TopSecret"), text);
    }

    @Test
    public void testTokenIsMaskedWhenRequestIsRejected() throws Exception {
        List<String> messages = captureLogs(() -> {
            Request request = new Request.Builder().get().url(BASE_URL + URL).build();
            try (Response response = networkClient.newCall(request).execute()) {
                assertEquals(401, response.code());
            }
        });

        // HttpServerHandler logs every request at DEBUG, and RestBaseAction the rejected request at WARN.
        assertTrue(messages.stream().anyMatch(m -> m.startsWith("request: url:[/api/show_proc?")
                && m.contains("token=*")), messages.toString());
        assertTrue(messages.stream().anyMatch(m -> m.startsWith("failed to process url: /api/show_proc?")
                && m.contains("token=*")), messages.toString());
        assertNoCredential(messages.toString());
    }

    @Test
    public void testTokenIsMaskedWhenActionLogsTheUri() throws Exception {
        // StaticResourceAction logs the uri at ERROR when the res parameter is missing.
        List<String> messages = captureLogs(() -> {
            Request request = new Request.Builder().get().addHeader("Authorization", rootAuth)
                    .url(BASE_URL + "/static?token=TopSecret").build();
            try (Response response = networkClient.newCall(request).execute()) {
                assertEquals(400, response.code());
            }
        });
        assertTrue(messages.contains("Wrong request without 'res' parameter. url: /static?token=*"),
                messages.toString());
        assertNoCredential(messages.toString());
    }

    @Test
    public void testTokenIsHiddenWhenUriIsMalformed() throws Exception {
        // %ZZ is not a valid escape, so routing the request fails to parse the uri. Sent through a raw socket,
        // since an HTTP client would escape the '%'.
        AtomicReference<String> response = new AtomicReference<>();
        List<String> messages = captureLogs(() -> response.set(sendRawRequest("GET " + URL
                + "&x=%ZZ HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")));
        assertTrue(response.get().startsWith("HTTP/1.1 400"), response.get());
        assertNoCredential(response.get());

        // Neither the uri nor the exception, whose message quotes the uri, is logged as is.
        assertTrue(messages.stream().anyMatch(m -> m.startsWith("accept bad request uri: /api/show_proc?<query not shown>")
                && m.contains("Malformed escape pair")), messages.toString());
        assertTrue(messages.stream().noneMatch(m -> m.startsWith("thrown: ")), messages.toString());
        assertNoCredential(messages.toString());
    }

    @Test
    public void testTokenIsMaskedWhenRequestCannotBeDecoded() throws Exception {
        // Netty fails to decode a request with an invalid Content-Length, after it has parsed the request line, so
        // the request keeps its uri, and HttpServerHandler rejects it with the decoder failure.
        AtomicReference<String> response = new AtomicReference<>();
        List<String> messages = captureLogs(() -> response.set(sendRawRequest("GET " + URL
                + " HTTP/1.1\r\nHost: localhost\r\nContent-Length: abc\r\nConnection: close\r\n\r\n")));
        assertTrue(response.get().startsWith("HTTP/1.1 400"), response.get());
        assertNoCredential(response.get());

        assertTrue(messages.stream().anyMatch(m -> m.startsWith("accept bad request: /api/show_proc?")
                && m.contains("token=*") && m.contains("Content-Length")), messages.toString());
        // The decoder failure and its exception are logged as well; they do not quote the uri either.
        assertTrue(messages.stream().anyMatch(m -> m.startsWith("thrown: ")), messages.toString());
        assertNoCredential(messages.toString());
    }

    @Test
    public void testSanitizeHttpReqUriForLog() {
        assertEquals("/api/show_proc?path=%2F&token=*", WebUtils.sanitizeHttpReqUriForLog(URL));

        // A uri that cannot be parsed is logged without its query.
        String unparsable = WebUtils.sanitizeHttpReqUriForLog("/api/show_proc?token=Top Secret|");
        assertTrue(unparsable.startsWith("/api/show_proc?<query not shown> (failed to sanitize uri: "), unparsable);
        assertFalse(unparsable.contains("Secret"), unparsable);

        assertEquals("/api/show_proc?<query not shown>", WebUtils.hideHttpReqQuery(URL));
        assertEquals("/api/show_proc", WebUtils.hideHttpReqQuery("/api/show_proc"));

        // What follows a '?' that java.net.URI does not take as a query is hidden too.
        assertEquals("http:foo?<query not shown>", WebUtils.sanitizeHttpReqUriForLog("http:foo?token=TopSecret"));
        assertEquals("/api#frag?<query not shown>", WebUtils.sanitizeHttpReqUriForLog("/api#frag?token=TopSecret"));
        assertEquals("/api/show_proc#frag", WebUtils.sanitizeHttpReqUriForLog("/api/show_proc#frag"));
    }

    @Test
    public void testTokenIsHiddenWhenItIsNotInTheQuery() throws Exception {
        // HTTP clients drop the fragment, so send the target as is through a raw socket. HttpServerHandler logs it
        // before routing.
        for (String target : List.of("/api/show_proc#frag?token=TopSecret", "http:foo?token=TopSecret")) {
            List<String> messages = captureLogs(() -> sendRawRequest("GET " + target
                    + " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"));
            assertTrue(messages.stream().anyMatch(m -> m.startsWith("request: url:[")
                    && m.contains("?<query not shown>")), messages.toString());
            assertNoCredential(messages.toString());
        }
    }
}
