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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.EOFException;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.naming.CommunicationException;
import javax.naming.ConfigurationException;
import javax.naming.InterruptedNamingException;
import javax.naming.InvalidNameException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.NamingSecurityException;
import javax.naming.OperationNotSupportedException;
import javax.naming.ServiceUnavailableException;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;

record LdapRetryPolicy(int connectTimeoutMs, int readTimeoutMs, int maxRetries, int retryIntervalMs) {
    private static final Logger LOG = LogManager.getLogger(LdapRetryPolicy.class);
    // The JNDI provider reports response read timeouts without a typed cause.
    private static final Pattern JNDI_READ_TIMEOUT =
            Pattern.compile("LDAP response read timed out, timeout used: [0-9]+ ms\\.");

    LdapRetryPolicy {
        if (connectTimeoutMs < 1 || connectTimeoutMs > 300000 || readTimeoutMs < 1 || readTimeoutMs > 300000 ||
                maxRetries < 0 || maxRetries > 5 || retryIntervalMs < 1 || retryIntervalMs > 10000) {
            throw new IllegalArgumentException("Invalid LDAP timeout or retry configuration");
        }
    }

    int maxAttempts() {
        return maxRetries + 1;
    }

    long retryDelayMs(int failedAttempt) {
        if (failedAttempt < 1 || failedAttempt > maxRetries) {
            throw new IllegalArgumentException("Invalid LDAP retry attempt");
        }
        return Math.min((long) retryIntervalMs << (failedAttempt - 1), 10000L);
    }

    @FunctionalInterface
    interface Operation<T> {
        T run() throws Exception;
    }

    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    <T> T execute(String stage, Operation<T> operation) throws Exception {
        return execute(stage, operation, Thread::sleep);
    }

    <T> T execute(String stage, Operation<T> operation, Sleeper sleeper) throws Exception {
        long startNanos = System.nanoTime();
        for (int attempt = 1; attempt <= maxAttempts(); attempt++) {
            checkInterrupted();
            try {
                T result = operation.run();
                if (attempt > 1) {
                    LOG.info("LDAP {} succeeded after {} attempts, maxAttempts={}, elapsedMs={}",
                            stage, attempt, maxAttempts(), elapsedMs(startNanos));
                }
                return result;
            } catch (NamingException e) {
                rethrowIfInterrupted(e);
                boolean retry = isRetryable(e) && attempt < maxAttempts();
                LOG.debug("LDAP {} attempt {}/{} failed, elapsedMs={}, retrying={}, failureType={}",
                        stage, attempt, maxAttempts(), elapsedMs(startNanos), retry, failureType(e));
                if (!retry) {
                    throw e;
                }
                LOG.debug("LDAP {} retry delayMs={}", stage, retryDelayMs(attempt));
                try {
                    sleeper.sleep(retryDelayMs(attempt));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    InterruptedNamingException failure = new InterruptedNamingException("LDAP authentication interrupted");
                    failure.setRootCause(interrupted);
                    throw failure;
                }
            }
        }
        throw new IllegalStateException("LDAP retry attempts exhausted without a result");
    }

    static long elapsedMs(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    static String failureType(Throwable failure) {
        Throwable root = failure;
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        while (root.getCause() != null && seen.add(root.getCause())) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName();
    }

    static boolean isRetryable(NamingException failure) {
        if (isInterruption(failure)) {
            return false;
        }
        boolean transientFailure = false;
        boolean tlsFailure = false;
        boolean transportFailure = false;
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable cause = failure; cause != null && seen.add(cause); cause = cause.getCause()) {
            if (cause instanceof NamingSecurityException || cause instanceof InvalidNameException ||
                    cause instanceof NameNotFoundException || cause instanceof ConfigurationException ||
                    cause instanceof OperationNotSupportedException || cause instanceof CertificateException ||
                    cause instanceof SSLPeerUnverifiedException || cause instanceof SSLProtocolException) {
                return false;
            }
            if (cause instanceof SSLException) {
                tlsFailure = true;
            }
            if (cause instanceof EOFException || cause instanceof SocketException || cause instanceof SocketTimeoutException) {
                transportFailure = true;
            }
            if (cause instanceof CommunicationException || cause instanceof ServiceUnavailableException ||
                    cause instanceof SocketException || cause instanceof SocketTimeoutException) {
                transientFailure = true;
            }
        }
        // TLS handshakes can fail because of transient transport errors, not just validation failures.
        if (tlsFailure) {
            return transportFailure;
        }
        return transientFailure || (failure.getClass() == NamingException.class && failure.getMessage() != null &&
                JNDI_READ_TIMEOUT.matcher(failure.getMessage()).matches());
    }

    static boolean isInterruption(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable cause = failure; cause != null && seen.add(cause); cause = cause.getCause()) {
            if (cause instanceof InterruptedNamingException || cause instanceof InterruptedException ||
                    (cause instanceof InterruptedIOException && !(cause instanceof SocketTimeoutException))) {
                return true;
            }
        }
        return Thread.currentThread().isInterrupted();
    }

    static void checkInterrupted() throws InterruptedNamingException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedNamingException("LDAP authentication interrupted");
        }
    }

    static void rethrowIfInterrupted(NamingException failure) throws InterruptedNamingException {
        if (isInterruption(failure)) {
            Thread.currentThread().interrupt();
            if (failure instanceof InterruptedNamingException interrupted) {
                throw interrupted;
            }
            InterruptedNamingException interrupted = new InterruptedNamingException("LDAP authentication interrupted");
            interrupted.setRootCause(failure);
            throw interrupted;
        }
    }
}
