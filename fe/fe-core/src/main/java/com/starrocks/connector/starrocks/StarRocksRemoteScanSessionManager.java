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

package com.starrocks.connector.starrocks;

import com.google.common.base.Strings;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class StarRocksRemoteScanSessionManager {
    private static final Logger LOG = LogManager.getLogger(StarRocksRemoteScanSessionManager.class);
    private static final int DEFAULT_CLEANUP_QUEUE_SIZE = 1024;
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final long DEFAULT_BATCH_DRAIN_WAIT_MS = 100;
    private static final Set<String> REGISTERED_CLEANUPS = ConcurrentHashMap.newKeySet();
    private static final ConcurrentMap<ConnectContext, ConcurrentMap<String, PreparedRemoteScanSession>>
            PREPARED_SCAN_SESSIONS =
            new ConcurrentHashMap<>();
    private static final CleanupClientFactory DEFAULT_CLEANUP_CLIENT_FACTORY =
            (feHttpUrl, scanTransport, feUser, fePassword, feHttpTimeoutMs, feHttpRetryTimes) -> {
                StarRocksFeClient client = new StarRocksFeClient(feHttpUrl, scanTransport, feUser, fePassword,
                        feHttpTimeoutMs, feHttpRetryTimes);
                return new StarRocksCleanupClient(client);
            };
    private static final CleanupScheduler DEFAULT_CLEANUP_SCHEDULER = new BoundedCleanupScheduler(
            DEFAULT_CLEANUP_QUEUE_SIZE, DEFAULT_BATCH_SIZE, DEFAULT_BATCH_DRAIN_WAIT_MS);
    private static volatile CleanupClientFactory cleanupClientFactory = DEFAULT_CLEANUP_CLIENT_FACTORY;
    private static volatile CleanupScheduler cleanupScheduler = DEFAULT_CLEANUP_SCHEDULER;

    private StarRocksRemoteScanSessionManager() {
    }

    public static void registerCleanupOnQueryFinished(ConnectContext context, String feHttpUrl,
                                                      String scanTransport, String feUser,
                                                      String fePassword, long feHttpTimeoutMs,
                                                      int feHttpRetryTimes, String sessionId) {
        if (context == null || Strings.isNullOrEmpty(feHttpUrl) || Strings.isNullOrEmpty(sessionId)) {
            return;
        }
        String normalizedFeHttpUrl = normalizeFeHttpUrl(feHttpUrl);
        String key = buildSessionKey(normalizedFeHttpUrl, sessionId);
        if (!REGISTERED_CLEANUPS.add(key)) {
            return;
        }

        context.registerListener(state -> {
            // Skipped when the session was already cleaned up eagerly (e.g. on a
            // planning failure) and unregistered — re-issuing the cleanup would just
            // produce a "session not found on any frontend" warn on the remote.
            if (!REGISTERED_CLEANUPS.contains(key)) {
                return;
            }
            boolean shouldCancel = shouldCancelRemoteScan(state);
            CleanupTask task = new CleanupTask(normalizedFeHttpUrl, scanTransport, feUser, fePassword,
                    feHttpTimeoutMs, feHttpRetryTimes, context, sessionId, shouldCancel, key);
            if (!cleanupScheduler.schedule(task)) {
                LOG.warn("failed to enqueue remote StarRocks scan session cleanup {} on {}", sessionId,
                        normalizedFeHttpUrl);
                removePreparedRemoteScans(context, normalizedFeHttpUrl, sessionId);
                REGISTERED_CLEANUPS.remove(key);
            }
        });
    }

    /**
     * Drops a session's finish-listener registration after it was cleaned up
     * eagerly, so query end does not re-issue the cleanup for a session that is
     * already gone from the remote.
     */
    public static void unregisterCleanup(ConnectContext context, String feHttpUrl, String sessionId) {
        if (context == null || Strings.isNullOrEmpty(feHttpUrl) || Strings.isNullOrEmpty(sessionId)) {
            return;
        }
        String normalizedFeHttpUrl = normalizeFeHttpUrl(feHttpUrl);
        REGISTERED_CLEANUPS.remove(buildSessionKey(normalizedFeHttpUrl, sessionId));
        removePreparedRemoteScans(context, normalizedFeHttpUrl, sessionId);
    }

    public static void registerPreparedRemoteScan(ConnectContext context, String feHttpUrl,
                                                  String scanTransport, String feUser,
                                                  String fePassword, long feHttpTimeoutMs,
                                                  int feHttpRetryTimes, String sessionId) {
        if (context == null || Strings.isNullOrEmpty(feHttpUrl) || Strings.isNullOrEmpty(sessionId)) {
            return;
        }
        String normalizedFeHttpUrl = normalizeFeHttpUrl(feHttpUrl);
        String key = buildSessionKey(normalizedFeHttpUrl, sessionId);
        PREPARED_SCAN_SESSIONS.computeIfAbsent(context, ignored -> new ConcurrentHashMap<>())
                .putIfAbsent(key, new PreparedRemoteScanSession(normalizedFeHttpUrl, scanTransport, feUser,
                        fePassword, feHttpTimeoutMs, feHttpRetryTimes, sessionId));
    }

    public static void startPreparedRemoteScans(ConnectContext context) throws StarRocksException {
        if (context == null) {
            return;
        }
        Map<String, PreparedRemoteScanSession> sessions = PREPARED_SCAN_SESSIONS.get(context);
        if (sessions == null || sessions.isEmpty()) {
            return;
        }
        List<PreparedRemoteScanSession> snapshot = new ArrayList<>(sessions.values());
        for (PreparedRemoteScanSession session : snapshot) {
            session.start();
        }
    }

    public static boolean hasPreparedRemoteScans(ConnectContext context) {
        if (context == null) {
            return false;
        }
        Map<String, PreparedRemoteScanSession> sessions = PREPARED_SCAN_SESSIONS.get(context);
        return sessions != null && !sessions.isEmpty();
    }

    static boolean shouldCancelRemoteScan(ConnectContext context) {
        if (context == null || context.isKilled()) {
            return true;
        }
        QueryState state = context.getState();
        return state != null && (state.isError() || state.getErrorCode() != null);
    }

    static void setCleanupSchedulerForTest(CleanupScheduler scheduler) {
        cleanupScheduler = scheduler;
    }

    static void setCleanupClientFactoryForTest(CleanupClientFactory factory) {
        cleanupClientFactory = factory;
    }

    static int registeredCleanupCountForTest() {
        return REGISTERED_CLEANUPS.size();
    }

    static void resetForTest() {
        REGISTERED_CLEANUPS.clear();
        PREPARED_SCAN_SESSIONS.clear();
        cleanupClientFactory = DEFAULT_CLEANUP_CLIENT_FACTORY;
        cleanupScheduler = DEFAULT_CLEANUP_SCHEDULER;
    }

    static void dispatchBatch(List<CleanupTask> tasks) {
        if (tasks == null || tasks.isEmpty()) {
            return;
        }
        Map<EndpointKey, List<CleanupTask>> grouped = new HashMap<>();
        for (CleanupTask task : tasks) {
            grouped.computeIfAbsent(task.endpointKey(), ignored -> new ArrayList<>()).add(task);
        }
        for (Map.Entry<EndpointKey, List<CleanupTask>> entry : grouped.entrySet()) {
            EndpointKey endpoint = entry.getKey();
            List<CleanupTask> group = entry.getValue();
            try {
                CleanupClient client = cleanupClientFactory.create(endpoint.feHttpUrl, endpoint.scanTransport,
                        endpoint.feUser, endpoint.fePassword, endpoint.feHttpTimeoutMs, endpoint.feHttpRetryTimes);
                List<StarRocksRemoteScanWire.CleanupItem> items = new ArrayList<>(group.size());
                for (CleanupTask task : group) {
                    items.add(new StarRocksRemoteScanWire.CleanupItem(task.sessionId, task.cancel));
                }
                client.batchCleanupScanSessions(items);
            } catch (Exception e) {
                List<String> sessionIds = new ArrayList<>(group.size());
                for (CleanupTask task : group) {
                    sessionIds.add(task.sessionId);
                }
                LOG.warn("failed to batch cleanup remote StarRocks scan sessions {} on {}",
                        sessionIds, endpoint.feHttpUrl, e);
            } finally {
                for (CleanupTask task : group) {
                    removePreparedRemoteScans(task.context, task.feHttpUrl, task.sessionId);
                    REGISTERED_CLEANUPS.remove(task.key);
                }
            }
        }
    }

    private static String normalizeFeHttpUrl(String feHttpUrl) {
        return feHttpUrl.endsWith("/") ? feHttpUrl.substring(0, feHttpUrl.length() - 1) : feHttpUrl;
    }

    private static String buildSessionKey(String normalizedFeHttpUrl, String sessionId) {
        return normalizedFeHttpUrl + "\t" + sessionId;
    }

    private static void removePreparedRemoteScans(ConnectContext context, String normalizedFeHttpUrl,
                                                  String sessionId) {
        ConcurrentMap<String, PreparedRemoteScanSession> sessions = PREPARED_SCAN_SESSIONS.get(context);
        if (sessions == null) {
            return;
        }
        sessions.remove(buildSessionKey(normalizedFeHttpUrl, sessionId));
        if (sessions.isEmpty()) {
            PREPARED_SCAN_SESSIONS.remove(context, sessions);
        }
    }

    private static class PreparedRemoteScanSession {
        private final String feHttpUrl;
        private final String scanTransport;
        private final String feUser;
        private final String fePassword;
        private final long feHttpTimeoutMs;
        private final int feHttpRetryTimes;
        private final String sessionId;
        private boolean started;

        private PreparedRemoteScanSession(String feHttpUrl, String scanTransport, String feUser,
                                          String fePassword, long feHttpTimeoutMs, int feHttpRetryTimes,
                                          String sessionId) {
            this.feHttpUrl = feHttpUrl;
            this.scanTransport = scanTransport;
            this.feUser = feUser;
            this.fePassword = fePassword;
            this.feHttpTimeoutMs = feHttpTimeoutMs;
            this.feHttpRetryTimes = feHttpRetryTimes;
            this.sessionId = sessionId;
        }

        private synchronized void start() throws StarRocksException {
            if (started) {
                return;
            }
            try {
                StarRocksFeClient client = new StarRocksFeClient(feHttpUrl, scanTransport, feUser, fePassword,
                        feHttpTimeoutMs, feHttpRetryTimes);
                client.startScanSession(sessionId);
                started = true;
            } catch (Exception e) {
                throw new StarRocksException(
                        "failed to start prepared remote StarRocks scan session " + sessionId + " on " + feHttpUrl, e);
            }
        }
    }

    interface CleanupClient {
        void batchCleanupScanSessions(List<StarRocksRemoteScanWire.CleanupItem> items);
    }

    interface CleanupClientFactory {
        CleanupClient create(String feHttpUrl, String scanTransport, String feUser, String fePassword,
                             long feHttpTimeoutMs, int feHttpRetryTimes);
    }

    interface CleanupScheduler {
        boolean schedule(CleanupTask task);

        int pendingTaskCount();
    }

    static final class CleanupTask {
        final String feHttpUrl;
        final String scanTransport;
        final String feUser;
        final String fePassword;
        final long feHttpTimeoutMs;
        final int feHttpRetryTimes;
        final ConnectContext context;
        final String sessionId;
        final boolean cancel;
        final String key;

        CleanupTask(String feHttpUrl, String scanTransport, String feUser, String fePassword,
                    long feHttpTimeoutMs, int feHttpRetryTimes, ConnectContext context,
                    String sessionId, boolean cancel, String key) {
            this.feHttpUrl = feHttpUrl;
            this.scanTransport = scanTransport;
            this.feUser = feUser;
            this.fePassword = fePassword;
            this.feHttpTimeoutMs = feHttpTimeoutMs;
            this.feHttpRetryTimes = feHttpRetryTimes;
            this.context = context;
            this.sessionId = sessionId;
            this.cancel = cancel;
            this.key = key;
        }

        EndpointKey endpointKey() {
            return new EndpointKey(feHttpUrl, scanTransport, feUser, fePassword, feHttpTimeoutMs, feHttpRetryTimes);
        }
    }

    private static final class EndpointKey {
        final String feHttpUrl;
        final String scanTransport;
        final String feUser;
        final String fePassword;
        final long feHttpTimeoutMs;
        final int feHttpRetryTimes;

        EndpointKey(String feHttpUrl, String scanTransport, String feUser, String fePassword,
                    long feHttpTimeoutMs, int feHttpRetryTimes) {
            this.feHttpUrl = feHttpUrl;
            this.scanTransport = scanTransport;
            this.feUser = feUser;
            this.fePassword = fePassword;
            this.feHttpTimeoutMs = feHttpTimeoutMs;
            this.feHttpRetryTimes = feHttpRetryTimes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EndpointKey)) {
                return false;
            }
            EndpointKey other = (EndpointKey) o;
            return feHttpTimeoutMs == other.feHttpTimeoutMs &&
                    feHttpRetryTimes == other.feHttpRetryTimes &&
                    Objects.equals(feHttpUrl, other.feHttpUrl) &&
                    Objects.equals(scanTransport, other.scanTransport) &&
                    Objects.equals(feUser, other.feUser) &&
                    Objects.equals(fePassword, other.fePassword);
        }

        @Override
        public int hashCode() {
            return Objects.hash(feHttpUrl, scanTransport, feUser, fePassword, feHttpTimeoutMs, feHttpRetryTimes);
        }
    }

    private static class StarRocksCleanupClient implements CleanupClient {
        private final StarRocksFeClient client;

        private StarRocksCleanupClient(StarRocksFeClient client) {
            this.client = client;
        }

        @Override
        public void batchCleanupScanSessions(List<StarRocksRemoteScanWire.CleanupItem> items) {
            client.batchCleanupScanSessions(items);
        }
    }

    private static class BoundedCleanupScheduler implements CleanupScheduler {
        private final BlockingQueue<CleanupTask> queue;
        private final int batchSize;
        private final long batchDrainWaitMs;

        private BoundedCleanupScheduler(int maxQueueSize, int batchSize, long batchDrainWaitMs) {
            queue = new ArrayBlockingQueue<>(maxQueueSize);
            this.batchSize = batchSize;
            this.batchDrainWaitMs = batchDrainWaitMs;
            Thread worker = new Thread(this::runLoop, "starrocks-remote-scan-cleanup");
            worker.setDaemon(true);
            worker.start();
        }

        @Override
        public boolean schedule(CleanupTask task) {
            return queue.offer(task);
        }

        @Override
        public int pendingTaskCount() {
            return queue.size();
        }

        private void runLoop() {
            while (true) {
                try {
                    CleanupTask first = queue.poll(batchDrainWaitMs, TimeUnit.MILLISECONDS);
                    if (first == null) {
                        continue;
                    }
                    List<CleanupTask> batch = new ArrayList<>();
                    batch.add(first);
                    queue.drainTo(batch, batchSize - 1);
                    dispatchBatch(batch);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    LOG.warn("remote StarRocks scan cleanup worker failed", t);
                }
            }
        }
    }
}
