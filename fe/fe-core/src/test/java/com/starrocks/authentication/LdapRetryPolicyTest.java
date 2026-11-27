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

package com.starrocks.authentication;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.naming.CommunicationException;
import javax.naming.ConfigurationException;
import javax.naming.Context;
import javax.naming.InterruptedNamingException;
import javax.naming.InvalidNameException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.NoPermissionException;
import javax.naming.ServiceUnavailableException;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.InvalidSearchFilterException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LdapRetryPolicyTest {
    private final LdapRetryPolicy policy = new LdapRetryPolicy(5000, 5000, 2, 200);

    @AfterEach
    void clearInterrupt() {
        Thread.interrupted();
    }

    @Test
    void testAdditionalRetryCount() {
        assertEquals(1, new LdapRetryPolicy(1, 1, 0, 1).maxAttempts());
        assertEquals(3, policy.maxAttempts());
        assertEquals(6, new LdapRetryPolicy(300000, 300000, 5, 10000).maxAttempts());
    }

    @Test
    void testRejectInvalidSettings() {
        int[][] invalid = { {0, 1, 0, 1}, {-1, 1, 0, 1}, {300001, 1, 0, 1}, {1, 0, 0, 1},
                {1, 300001, 0, 1}, {1, 1, -1, 1}, {1, 1, 6, 1}, {1, 1, 0, 0}, {1, 1, 0, 10001}};
        for (int[] values : invalid) {
            assertThrows(IllegalArgumentException.class,
                    () -> new LdapRetryPolicy(values[0], values[1], values[2], values[3]));
        }
    }

    @Test
    void testNoRetry() {
        AtomicInteger calls = new AtomicInteger();
        List<Long> delays = new ArrayList<>();
        CommunicationException failure = new CommunicationException();
        assertSame(failure, assertThrows(CommunicationException.class,
                () -> new LdapRetryPolicy(1, 1, 0, 1).execute("user bind", () -> {
                    calls.incrementAndGet();
                    throw failure;
                }, delays::add)));
        assertEquals(1, calls.get());
        assertTrue(delays.isEmpty());
    }

    @Test
    void testTransientFailureThenSuccess() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        List<Long> delays = new ArrayList<>();
        String result = policy.execute("user bind", () -> {
            if (calls.incrementAndGet() == 1) {
                throw new CommunicationException();
            }
            return "success";
        }, delays::add);
        assertEquals("success", result);
        assertEquals(2, calls.get());
        assertEquals(List.of(200L), delays);
    }

    @Test
    void testRetryExhaustion() {
        AtomicInteger calls = new AtomicInteger();
        List<Long> delays = new ArrayList<>();
        CommunicationException failure = new CommunicationException();
        assertSame(failure, assertThrows(CommunicationException.class, () -> policy.execute("root bind/search", () -> {
            calls.incrementAndGet();
            throw failure;
        }, delays::add)));
        assertEquals(3, calls.get());
        assertEquals(List.of(200L, 400L), delays);
    }

    @Test
    void testCappedBackoff() {
        List<Long> delays = new ArrayList<>();
        LdapRetryPolicy capped = new LdapRetryPolicy(1, 1, 5, 8000);
        assertThrows(CommunicationException.class, () -> capped.execute("user bind", () -> {
            throw new CommunicationException();
        }, delays::add));
        assertEquals(List.of(8000L, 10000L, 10000L, 10000L, 10000L), delays);
        assertThrows(IllegalArgumentException.class, () -> capped.retryDelayMs(0));
        assertThrows(IllegalArgumentException.class, () -> capped.retryDelayMs(6));
    }

    @Test
    void testPermanentNamingFailuresAreNotRetried() {
        List<NamingException> failures = List.of(new javax.naming.AuthenticationException(), new NoPermissionException(),
                new InvalidNameException(), new NameNotFoundException(), new ConfigurationException(),
                new InvalidSearchFilterException(), new NamingException("unrecognized failure"));
        for (NamingException failure : failures) {
            AtomicInteger calls = new AtomicInteger();
            assertSame(failure, assertThrows(NamingException.class, () -> policy.execute("user bind", () -> {
                calls.incrementAndGet();
                throw failure;
            }, millis -> {
                throw new AssertionError("Permanent failures must not wait for retries");
            })));
            assertEquals(1, calls.get());
        }
    }

    @Test
    void testTlsValidationAndUnknownFailuresAreNotRetried() {
        for (Exception cause : List.of(new SSLHandshakeException("unknown handshake failure"), new CertificateException(),
                new SSLPeerUnverifiedException("peer not verified"), new SSLProtocolException("invalid protocol"))) {
            CommunicationException failure = tlsFailure(cause);
            AtomicInteger calls = new AtomicInteger();
            assertSame(failure, assertThrows(CommunicationException.class, () -> policy.execute("user bind", () -> {
                calls.incrementAndGet();
                throw failure;
            }, millis -> {
                throw new AssertionError("TLS validation failures must not wait for retries");
            })));
            assertEquals(1, calls.get());
        }
    }

    @Test
    void testTlsTransportFailureThenSuccess() throws Exception {
        for (Exception cause : List.of(new EOFException(), new SocketException("Connection reset"),
                new SocketTimeoutException())) {
            CommunicationException failure = tlsFailure(cause);
            assertTrue(LdapRetryPolicy.isRetryable(failure));
            assertFalse(LdapRetryPolicy.isInterruption(failure));
            AtomicInteger calls = new AtomicInteger();
            List<Long> delays = new ArrayList<>();
            assertEquals("success", policy.execute("user bind", () -> {
                if (calls.incrementAndGet() == 1) {
                    throw failure;
                }
                return "success";
            }, delays::add));
            assertEquals(2, calls.get());
            assertEquals(List.of(200L), delays);
        }
    }

    @Test
    void testTlsValidationFailureOverridesTransportCause() {
        for (Exception cause : List.of(new CertificateException(), new SSLPeerUnverifiedException("peer not verified"),
                new SSLProtocolException("invalid protocol"))) {
            cause.initCause(new EOFException());
            assertFalse(LdapRetryPolicy.isRetryable(tlsFailure(cause)));
        }
    }

    @Test
    void testTlsInterruptionIsNotRetried() {
        CommunicationException failure = tlsFailure(new InterruptedIOException());
        assertFalse(LdapRetryPolicy.isRetryable(failure));
        AtomicInteger calls = new AtomicInteger();
        InterruptedNamingException interrupted = assertThrows(InterruptedNamingException.class,
                () -> policy.execute("user bind", () -> {
                    calls.incrementAndGet();
                    throw failure;
                }));
        assertSame(failure, interrupted.getRootCause());
        assertEquals(1, calls.get());
        assertTrue(Thread.currentThread().isInterrupted());
    }

    private static CommunicationException tlsFailure(Exception cause) {
        SSLHandshakeException handshake = new SSLHandshakeException("TLS handshake failed");
        handshake.initCause(cause);
        CommunicationException failure = new CommunicationException();
        failure.setRootCause(handshake);
        return failure;
    }

    @Test
    void testJndiTlsHandshakeDisconnectHasBoundedRetries() throws Exception {
        LdapRetryPolicy localPolicy = new LdapRetryPolicy(2000, 2000, 2, 1);
        AtomicInteger calls = new AtomicInteger();
        List<Long> delays = new ArrayList<>();
        Thread peer = null;
        try (ServerSocket server = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"))) {
            server.setSoTimeout(5000);
            FutureTask<Void> disconnects = new FutureTask<>(() -> {
                for (int attempt = 0; attempt < localPolicy.maxAttempts(); attempt++) {
                    try (Socket socket = server.accept()) {
                        socket.setSoTimeout(2000);
                        // Consume ClientHello before closing so JSSE observes a handshake EOF.
                        DataInputStream input = new DataInputStream(socket.getInputStream());
                        input.readUnsignedByte();
                        input.readUnsignedShort();
                        int length = input.readUnsignedShort();
                        assertEquals(length, input.readNBytes(length).length);
                    }
                }
                return null;
            });
            peer = new Thread(disconnects, "ldap-test-tls-disconnect");
            peer.setDaemon(true);
            peer.start();

            Hashtable<String, String> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(Context.PROVIDER_URL, "ldaps://127.0.0.1:" + server.getLocalPort());
            env.put("com.sun.jndi.ldap.connect.timeout", Integer.toString(localPolicy.connectTimeoutMs()));
            env.put("com.sun.jndi.ldap.read.timeout", Integer.toString(localPolicy.readTimeoutMs()));
            CommunicationException failure = assertThrows(CommunicationException.class,
                    () -> localPolicy.execute("user bind", () -> {
                        calls.incrementAndGet();
                        InitialDirContext context = new InitialDirContext(env);
                        context.close();
                        return null;
                    }, delays::add));
            assertTrue(failure.getRootCause() instanceof SSLHandshakeException);
            assertTrue(failure.getRootCause().getCause() instanceof EOFException);
            assertEquals(3, calls.get());
            assertEquals(List.of(1L, 2L), delays);
            disconnects.get(5, TimeUnit.SECONDS);
        } finally {
            if (peer != null) {
                peer.join(5000);
                assertFalse(peer.isAlive());
            }
        }
    }

    @Test
    void testWrappedAuthenticationFailureIsNotRetried() {
        CommunicationException failure = new CommunicationException();
        failure.setRootCause(new javax.naming.AuthenticationException());
        assertFalse(LdapRetryPolicy.isRetryable(failure));
    }

    @Test
    void testRecognizedResponseTimeoutIsRetried() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        policy.execute("root bind/search", () -> {
            if (calls.incrementAndGet() == 1) {
                throw new NamingException("LDAP response read timed out, timeout used: 5000 ms.");
            }
            return null;
        }, millis -> { });
        assertEquals(2, calls.get());
        assertFalse(LdapRetryPolicy.isRetryable(new NamingException("LDAP response read timed out unexpectedly")));
    }

    @Test
    void testServiceUnavailableIsRetryable() {
        assertTrue(LdapRetryPolicy.isRetryable(new ServiceUnavailableException()));
    }

    @Test
    void testSocketTimeoutDoesNotCountAsInterruption() {
        CommunicationException failure = new CommunicationException();
        failure.setRootCause(new SocketTimeoutException());
        assertTrue(LdapRetryPolicy.isRetryable(failure));
        assertFalse(LdapRetryPolicy.isInterruption(failure));
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    void testPreInterruptedThreadDoesNotAttemptOperation() {
        AtomicInteger calls = new AtomicInteger();
        Thread.currentThread().interrupt();
        assertThrows(InterruptedNamingException.class, () -> policy.execute("user bind", () -> {
            calls.incrementAndGet();
            return null;
        }));
        assertEquals(0, calls.get());
        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    void testNamingInterruptionIsNotRetried() {
        AtomicInteger calls = new AtomicInteger();
        InterruptedNamingException failure = new InterruptedNamingException();
        assertSame(failure, assertThrows(InterruptedNamingException.class, () -> policy.execute("user bind", () -> {
            calls.incrementAndGet();
            throw failure;
        })));
        assertEquals(1, calls.get());
        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    void testWrappedIoInterruptionIsNotRetried() {
        CommunicationException failure = new CommunicationException();
        failure.setRootCause(new InterruptedIOException());
        InterruptedNamingException interrupted = assertThrows(InterruptedNamingException.class,
                () -> policy.execute("user bind", () -> {
                    throw failure;
                }));
        assertSame(failure, interrupted.getRootCause());
        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    void testBackoffInterruptionStopsRetries() {
        AtomicInteger calls = new AtomicInteger();
        InterruptedException interruption = new InterruptedException();
        InterruptedNamingException failure = assertThrows(InterruptedNamingException.class,
                () -> policy.execute("user bind", () -> {
                    calls.incrementAndGet();
                    throw new CommunicationException();
                }, millis -> {
                    throw interruption;
                }));
        assertSame(interruption, failure.getRootCause());
        assertEquals(1, calls.get());
        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    void testInterruptionBetweenAttemptsStopsRetries() {
        AtomicInteger calls = new AtomicInteger();
        assertThrows(InterruptedNamingException.class, () -> policy.execute("user bind", () -> {
            calls.incrementAndGet();
            throw new CommunicationException();
        }, millis -> Thread.currentThread().interrupt()));
        assertEquals(1, calls.get());
        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    void testCauseCycleDoesNotLoop() {
        CommunicationException outer = new CommunicationException();
        NamingException inner = new NamingException();
        outer.setRootCause(inner);
        inner.setRootCause(outer);
        assertTrue(LdapRetryPolicy.isRetryable(outer));
        assertFalse(LdapRetryPolicy.isInterruption(outer));
        assertEquals("CommunicationException", LdapRetryPolicy.failureType(outer));
    }

    @Test
    void testRecoveryLogUsesActualAttemptCount() throws Exception {
        try (LogCapture capture = new LogCapture()) {
            AtomicInteger calls = new AtomicInteger();
            policy.execute("user bind", () -> {
                if (calls.incrementAndGet() == 1) {
                    throw new CommunicationException();
                }
                return null;
            }, millis -> { });
            assertTrue(capture.messages().stream().anyMatch(m ->
                    m.contains("succeeded after 2 attempts, maxAttempts=3, elapsedMs=")));
            assertFalse(capture.events.stream().anyMatch(e -> e.getLevel().isMoreSpecificThan(Level.WARN)));
        }
    }

    @Test
    void testPermanentFailureLogDoesNotClaimRetriesOrExposeCredentials() {
        try (LogCapture capture = new LogCapture()) {
            assertThrows(javax.naming.AuthenticationException.class, () -> policy.execute("user bind", () -> {
                throw new javax.naming.AuthenticationException("secret-password");
            }));
            assertTrue(capture.messages().stream().anyMatch(m -> m.contains("attempt 1/3 failed") &&
                    m.contains("retrying=false")));
            assertFalse(capture.messages().stream().anyMatch(m -> m.contains("secret-password")));
            assertTrue(capture.events.stream().allMatch(e -> e.getLevel() == Level.DEBUG && e.getThrown() == null));
        }
    }

    private static class LogCapture extends AbstractAppender implements AutoCloseable {
        private final List<LogEvent> events = new ArrayList<>();
        private final Logger logger = (Logger) LogManager.getLogger(LdapRetryPolicy.class);
        private final Level previousLevel = logger.getLevel();

        LogCapture() {
            super("ldap-retry-test", null, null, false, Property.EMPTY_ARRAY);
            start();
            logger.addAppender(this);
            logger.setLevel(Level.DEBUG);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }

        List<String> messages() {
            return events.stream().map(e -> e.getMessage().getFormattedMessage()).toList();
        }

        @Override
        public void close() {
            logger.removeAppender(this);
            logger.setLevel(previousLevel);
            stop();
        }
    }
}
