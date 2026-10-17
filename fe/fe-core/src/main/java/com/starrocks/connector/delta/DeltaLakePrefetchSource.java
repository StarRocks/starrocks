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

package com.starrocks.connector.delta;

import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

// One consumer; close may run concurrently. At most one consumed batch and one prefetched batch are retained.
// Workers produce a finite batch and return, never blocking on a full producer queue.
public final class DeltaLakePrefetchSource implements RemoteFileInfoSource {
    private static final Logger LOG = LogManager.getLogger(DeltaLakePrefetchSource.class);
    private static final int DEFAULT_BATCH_FILES = 256;
    private static final long DEFAULT_BATCH_BYTES = 1024 * 1024;

    private record Batch(List<RemoteFileInfo> files, boolean finished) { }

    private final RemoteFileInfoSource delegate;
    private final Executor executor;
    private final int batchFiles;
    private final long batchBytes;
    private final Tracers ownerTracers = Tracers.get();
    private final ConnectContext context = ConnectContext.get();
    private final Object stateLock = new Object();
    private final ReentrantLock readerLock = new ReentrantLock();
    private final ArrayDeque<RemoteFileInfo> ready = new ArrayDeque<>();
    private FutureTask<Batch> pending;
    private boolean finished;
    private boolean readerClosed;
    private volatile boolean closed;

    public DeltaLakePrefetchSource(RemoteFileInfoSource delegate, Executor executor) {
        this(delegate, executor, DEFAULT_BATCH_FILES, DEFAULT_BATCH_BYTES);
    }

    DeltaLakePrefetchSource(RemoteFileInfoSource delegate, Executor executor, int batchFiles, long batchBytes) {
        if (batchFiles <= 0 || batchBytes <= 0) {
            throw new IllegalArgumentException("Delta prefetch batch limits must be positive");
        }
        this.delegate = delegate;
        this.executor = executor;
        this.batchFiles = batchFiles;
        this.batchBytes = batchBytes;
    }

    // Submit outside stateLock: cancellation must remain possible during synchronous fallback I/O.
    private void submit(FutureTask<Batch> task) {
        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            Tracers.record(ownerTracers, EXTERNAL, "DELTA_LAKE.prefetchFallback", "caller");
            task.run();
        }
    }

    private Batch readBatch() {
        Tracers previous = Tracers.get();
        Tracers worker = ownerTracers.fork(false);
        Tracers.set(worker);
        boolean terminal = false;
        readerLock.lock();
        try (ConnectContext.ScopeGuard scope = context == null ? null : context.bindScope();
                Timer ignored = Tracers.watchScope(EXTERNAL, "DELTA_LAKE.prefetchRead")) {
            List<RemoteFileInfo> files = new ArrayList<>();
            long bytes = 0;
            while (!closed && files.size() < batchFiles && bytes < batchBytes) {
                if (!delegate.hasMoreOutput()) {
                    terminal = true;
                    return new Batch(files, true);
                }
                if (closed) {
                    break;
                }
                RemoteFileInfo file = delegate.getOutput();
                files.add(file);
                bytes += estimateBytes((DeltaRemoteFileInfo) file);
            }
            return new Batch(files, closed);
        } catch (RuntimeException | Error e) {
            terminal = true;
            throw e;
        } finally {
            readerLock.unlock();
            // Check AFTER unlocking: either close() acquired the idle reader, or we observe its request here.
            if (closed || terminal) {
                closeReaderIfIdle();
            }
            ownerTracers.mergeFrom(worker);
            Tracers.set(previous);
        }
    }

    // A batch can exceed this estimate by one oversized file; file count is always strictly bounded.
    private static long estimateBytes(DeltaRemoteFileInfo info) {
        FileScanTask file = info.getFileScanTask();
        long bytes = 256L + 2L * file.getFileStatus().getPath().length();
        for (Map.Entry<String, String> entry : file.getPartitionValues().entrySet()) {
            bytes += 96L + (entry.getKey() == null ? 0 : 2L * entry.getKey().length())
                    + (entry.getValue() == null ? 0 : 2L * entry.getValue().length());
        }
        if (file.getDv() != null) {
            bytes += 128L + 2L * file.getDv().getPathOrInlineDv().length();
        }
        return bytes;
    }

    @Override
    public boolean hasMoreOutput() {
        FutureTask<Batch> future;
        boolean start = false;
        synchronized (stateLock) {
            if (closed) {
                return false;
            }
            if (!ready.isEmpty()) {
                return true;
            }
            if (finished) {
                return false;
            }
            if (pending == null) {
                pending = new FutureTask<>(this::readBatch);
                start = true;
            }
            future = pending;
        }
        if (start) {
            submit(future);
        }
        Batch batch;
        try (Timer ignored = Tracers.watchScope(ownerTracers, EXTERNAL, "DELTA_LAKE.prefetchWait")) {
            batch = future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            close();
            throw new StarRocksConnectorException("Interrupted waiting for Delta file prefetch", e);
        } catch (CancellationException e) {
            if (closed) {
                return false;
            }
            close();
            throw new StarRocksConnectorException("Delta file prefetch was canceled", e);
        } catch (ExecutionException e) {
            close();
            throw new StarRocksConnectorException("Failed to prefetch Delta files", e.getCause());
        }
        FutureTask<Batch> next = null;
        boolean hasOutput;
        synchronized (stateLock) {
            if (closed) {
                return false;
            }
            pending = null;
            ready.addAll(batch.files());
            finished = batch.finished();
            if (!finished) {
                next = new FutureTask<>(this::readBatch);
                pending = next;
            }
            hasOutput = !ready.isEmpty();
        }
        if (next != null) {
            submit(next);
        }
        return hasOutput && !closed;
    }

    @Override
    public RemoteFileInfo getOutput() {
        if (!hasMoreOutput()) {
            throw new NoSuchElementException("Delta file source is exhausted or closed");
        }
        synchronized (stateLock) {
            if (closed) {
                throw new NoSuchElementException("Delta file source is closed");
            }
            return ready.removeFirst();
        }
    }

    private void closeReaderIfIdle() {
        if (readerLock.tryLock()) {
            try {
                if (!readerClosed) {
                    readerClosed = true;
                    delegate.close();
                }
            } catch (Exception e) {
                LOG.warn("Failed to close Delta prefetch reader", e);
            } finally {
                readerLock.unlock();
            }
        }
    }

    @Override
    public void close() {
        synchronized (stateLock) {
            if (closed) {
                return;
            }
            closed = true;
            ready.clear();
            if (pending != null) {
                pending.cancel(true);
                pending = null;
            }
        }
        Tracers.record(ownerTracers, EXTERNAL, "DELTA_LAKE.prefetchClosed", "true");
        closeReaderIfIdle();
    }
}
