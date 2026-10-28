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

import com.starrocks.common.Config;
import com.starrocks.common.util.CredentialMask;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.SetConfigAction;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SetConfigActionTest extends StarRocksHttpTestCase {

    // Collects the messages logged to the loggers it is added to, and the exceptions logged with them.
    private static class MessageCollector extends AbstractAppender {
        private final List<String> messages = new CopyOnWriteArrayList<>();

        MessageCollector() {
            super("set-config-action-test-message-collector", null, null);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
            if (event.getThrown() != null) {
                messages.add("thrown: " + event.getThrown());
            }
        }
    }

    @TempDir
    Path tempDir;

    @BeforeEach
    public void initConfig() throws Exception {
        // /api/_set_config and WebUtils.sanitizeHttpReqUri look configs up in what Config.init() registers.
        URL resource = getClass().getClassLoader().getResource("conf/config_test.properties");
        assert resource != null;
        Path confFile = tempDir.resolve("fe.conf");
        Files.copy(Paths.get(resource.toURI()), confFile);
        new Config().init(confFile.toString());
    }

    // Captures, at DEBUG, what the HTTP server, the REST base action and SetConfigAction log while running body.
    private static List<String> captureLogs(ThrowingRunnable body) throws Exception {
        MessageCollector collector = new MessageCollector();
        List<Logger> loggers = List.of(
                (Logger) LogManager.getLogger(HttpServerHandler.class),
                (Logger) LogManager.getLogger(BaseAction.class),
                (Logger) LogManager.getLogger(RestBaseAction.class),
                (Logger) LogManager.getLogger(SetConfigAction.class));
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

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    // The value of aws_s3_secret_key holds an escaped '&': FakeS3Secret&TailSecret.
    private static final String SET_CONFIG_URL = "/api/_set_config?authentication_ldap_simple_bind_root_pwd=FakeLdapPwd123"
            + "&aws_s3_secret_key=FakeS3Secret%26TailSecret&qe_slow_log_ms=5001";

    private static void assertNoCredential(String text) {
        assertFalse(text.contains("FakeLdapPwd123"), text);
        assertFalse(text.contains("FakeS3Secret"), text);
        assertFalse(text.contains("TailSecret"), text);
        assertFalse(text.contains("TopSecret"), text);
    }

    @Test
    public void testCredentialValueIsMaskedInLogAndReply() throws Exception {
        long slowLogMs = Config.qe_slow_log_ms;
        AtomicReference<Map<String, Object>> body = new AtomicReference<>();
        List<String> messages;
        try {
            // authentication_ldap_simple_bind_root_pwd is a mutable credential config, aws_s3_secret_key an immutable
            // one (so it is reported in "err"), and qe_slow_log_ms is not a credential.
            messages = captureLogs(() -> {
                Request request = new Request.Builder()
                        .get()
                        .addHeader("Authorization", rootAuth)
                        .url(BASE_URL + SET_CONFIG_URL)
                        .build();
                try (Response response = networkClient.newCall(request).execute()) {
                    body.set(parseResponseBody(response));
                }
            });
            // The value is still set; only what is reported is masked.
            assertEquals("FakeLdapPwd123", Config.authentication_ldap_simple_bind_root_pwd);
            assertEquals(5001, Config.qe_slow_log_ms);
        } finally {
            Config.authentication_ldap_simple_bind_root_pwd = "";
            Config.qe_slow_log_ms = slowLogMs;
        }

        Map<?, ?> set = (Map<?, ?>) body.get().get("set");
        Map<?, ?> err = (Map<?, ?>) body.get().get("err");
        assertEquals(CredentialMask.LONG, set.get("authentication_ldap_simple_bind_root_pwd"), body.get().toString());
        assertEquals("5001", set.get("qe_slow_log_ms"), body.get().toString());
        assertEquals("[" + CredentialMask.LONG + "]", err.get("aws_s3_secret_key"), body.get().toString());
        assertNoCredential(body.get().toString());

        // HttpServerHandler logs the request uri, SetConfigAction the parameters.
        assertTrue(messages.stream().anyMatch(m -> m.startsWith("request: url:[/api/_set_config?")
                && m.contains("authentication_ldap_simple_bind_root_pwd=" + CredentialMask.LONG)
                && m.contains("qe_slow_log_ms=5001")), messages.toString());
        assertTrue(messages.stream().anyMatch(m -> m.startsWith("get config from url: ")
                && m.contains("qe_slow_log_ms=[5001]")), messages.toString());
        assertNoCredential(messages.toString());
    }

    @Test
    public void testCredentialValueIsMaskedWhenRequestIsRejected() throws Exception {
        String ldapPwd = Config.authentication_ldap_simple_bind_root_pwd;
        // The actions read ';' as a separator too, and skip a leading '=' in a name, so the last two pass
        // aws_s3_secret_key=TopSecret as well. Masking follows the same parsing.
        Map<String, String> maskedParams = Map.of(
                SET_CONFIG_URL, "authentication_ldap_simple_bind_root_pwd=" + CredentialMask.LONG,
                "/api/_set_config?qe_slow_log_ms=1;aws_s3_secret_key=TopSecret",
                "qe_slow_log_ms=1&aws_s3_secret_key=" + CredentialMask.LONG,
                "/api/_set_config?=aws_s3_secret_key=TopSecret", "?aws_s3_secret_key=" + CredentialMask.LONG);
        for (Map.Entry<String, String> entry : maskedParams.entrySet()) {
            // No auth information: HttpServerHandler, BaseAction and RestBaseAction log the uri, and the request
            // is rejected.
            List<String> messages = captureLogs(() -> {
                Request request = new Request.Builder().get().url(BASE_URL + entry.getKey()).build();
                try (Response response = networkClient.newCall(request).execute()) {
                    assertEquals(401, response.code());
                }
            });
            assertEquals(ldapPwd, Config.authentication_ldap_simple_bind_root_pwd);

            assertTrue(messages.stream().anyMatch(m -> m.startsWith("request: url:[/api/_set_config?")
                    && m.contains(entry.getValue())), messages.toString());
            assertTrue(messages.stream().anyMatch(m -> m.startsWith("parse auth info failed, url /api/_set_config?")
                    && m.contains(entry.getValue())), messages.toString());
            assertTrue(messages.stream().anyMatch(m -> m.startsWith("failed to process url: /api/_set_config?")
                    && m.contains(entry.getValue())), messages.toString());
            assertNoCredential(messages.toString());
        }
    }

    @Test
    public void testSanitizeHttpReqUri() throws Exception {
        // The uri is rebuilt from the parameters as the actions parse them: the whole value of aws_s3_secret_key is
        // masked, including what follows its escaped '&'. An empty credential stays empty, as in ADMIN SHOW
        // FRONTEND CONFIG. The other parameters keep their order, and are encoded again.
        String sanitized = WebUtils.sanitizeHttpReqUri(SET_CONFIG_URL
                + "&authentication_ldap_simple_ssl_conn_trust_store_pwd=&label=a%20b&flag");
        assertEquals("/api/_set_config?authentication_ldap_simple_bind_root_pwd=" + CredentialMask.LONG
                + "&aws_s3_secret_key=" + CredentialMask.LONG + "&qe_slow_log_ms=5001"
                + "&authentication_ldap_simple_ssl_conn_trust_store_pwd=&label=a+b&flag=", sanitized);

        // However the query is written, a value read as a credential config is masked.
        assertEquals("/api/_set_config?qe_slow_log_ms=1&aws_s3_secret_key=" + CredentialMask.LONG,
                WebUtils.sanitizeHttpReqUri("/api/_set_config?qe_slow_log_ms=1;aws_s3_secret_key=TopSecret"));
        assertEquals("/api/_set_config?aws_s3_secret_key=" + CredentialMask.LONG,
                WebUtils.sanitizeHttpReqUri("/api/_set_config?=aws_s3_secret_key=TopSecret"));
        assertEquals("/api/_set_config?aws_s3_secret_key=" + CredentialMask.LONG,
                WebUtils.sanitizeHttpReqUri("/api/_set_config?aws_s3_secret%5Fkey=FakeS3Secret"));

        // Config names are only masked for /api/_set_config. What is not parsed as a parameter, like the fragment,
        // is left out.
        assertEquals("/api/other?authentication_ldap_simple_bind_root_pwd=v",
                WebUtils.sanitizeHttpReqUri("/api/other?authentication_ldap_simple_bind_root_pwd=v"));
        assertEquals("http://fe:8030/api/bootstrap?token=*&cluster_id=*",
                WebUtils.sanitizeHttpReqUri("http://fe:8030/api/bootstrap?token=head%26TailSecret&cluster_id=1#top"));
        assertEquals("/api/metrics", WebUtils.sanitizeHttpReqUri("/api/metrics"));
    }
}
