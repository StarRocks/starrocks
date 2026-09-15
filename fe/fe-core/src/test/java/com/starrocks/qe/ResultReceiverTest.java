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

package com.starrocks.qe;

import com.starrocks.common.Status;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.proto.StatusPB;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.PFetchDataRequest;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class ResultReceiverTest {

    private static final TNetworkAddress ADDRESS = new TNetworkAddress("127.0.0.1", 8060);
    private static final long BACKEND_ID = 10001L;

    // A stand-in for the brpc fetch_data future that reproduces jprotobuf-rpc-core's real, non-standard
    // behavior: get(timeout) does NOT throw a bare TimeoutException when a slice elapses; it throws
    // ExecutionException wrapping a TimeoutException. A plain CompletableFuture (used by an earlier test)
    // throws the standard TimeoutException and therefore hides the bug this class exists to guard against.
    private static final class BrpcLikeFuture implements Future<PFetchDataResult> {
        // Number of get() calls that report a slice timeout before the result is delivered.
        // Integer.MAX_VALUE models a sink backend that is gone and will never answer.
        private final int timeoutsBeforeResult;
        private final PFetchDataResult result;
        private int calls = 0;

        private BrpcLikeFuture(int timeoutsBeforeResult, PFetchDataResult result) {
            this.timeoutsBeforeResult = timeoutsBeforeResult;
            this.result = result;
        }

        static BrpcLikeFuture neverAnswers() {
            return new BrpcLikeFuture(Integer.MAX_VALUE, null);
        }

        static BrpcLikeFuture answersAfter(int timeouts, PFetchDataResult result) {
            return new BrpcLikeFuture(timeouts, result);
        }

        @Override
        public PFetchDataResult get(long timeout, TimeUnit unit) throws ExecutionException, TimeoutException {
            if (calls < timeoutsBeforeResult) {
                calls++;
                // Sleep a bounded slice so the polling loop does not busy-spin, then fail the way the
                // real client does: TimeoutException wrapped in ExecutionException.
                try {
                    Thread.sleep(Math.min(unit.toMillis(timeout), 50L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new ExecutionException("Ocurrs time out with specfied time " + timeout + " MILLISECONDS",
                        new TimeoutException("timeout"));
            }
            return result;
        }

        @Override
        public PFetchDataResult get() {
            throw new UnsupportedOperationException();
        }

        // jprotobuf's future returns false here and does nothing — cancelling the future cannot unblock us.
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return calls >= timeoutsBeforeResult;
        }
    }

    private static PFetchDataResult okEosResult() {
        PFetchDataResult result = new PFetchDataResult();
        result.status = new StatusPB();
        result.status.statusCode = TStatusCode.OK.getValue();
        result.packetSeq = 0L;
        result.eos = true;
        return result;
    }

    private static void mockFetchData(BackendServiceClient client, Future<PFetchDataResult> future) throws Exception {
        new Expectations() {
            {
                BackendServiceClient.getInstance();
                result = client;
                minTimes = 0;

                client.fetchDataAsync((TNetworkAddress) any, (PFetchDataRequest) any);
                result = future;
                minTimes = 0;
            }
        };
    }

    // The coordinator cancels the query (e.g. CoordinatorMonitor saw the result-sink backend die) while the
    // query thread is blocked waiting for rows. cancel() only flips a flag, so the wait must observe it on
    // its own — otherwise the thread, and the admission slot it holds, stay busy until query_timeout.
    @Test
    public void testCancelWakesUpGetNextDespiteBrpcTimeoutWrapping(@Mocked BackendServiceClient client)
            throws Exception {
        mockFetchData(client, BrpcLikeFuture.neverAnswers());

        final int queryTimeoutMs = 600_000;
        ResultReceiver receiver = new ResultReceiver(new TUniqueId(1L, 2L), BACKEND_ID, ADDRESS, queryTimeoutMs);
        Status status = new Status();
        AtomicReference<RowBatch> batch = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread queryThread = new Thread(() -> {
            try {
                batch.set(receiver.getNext(status));
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "result-receiver-test-query");
        queryThread.start();

        // Let the query thread reach the wait on the RPC future.
        Thread.sleep(500);
        Assertions.assertTrue(queryThread.isAlive(), "getNext must block while the BE has not answered");

        long cancelledAtMs = System.currentTimeMillis();
        receiver.cancel();
        queryThread.join(10_000);

        Assertions.assertFalse(queryThread.isAlive(), "getNext must return once the receiver is cancelled");
        Assertions.assertNull(failure.get(), "getNext must not throw on cancel: " + failure.get());
        Assertions.assertTrue(status.isCancelled(), "status after cancel: " + status);
        Assertions.assertNull(batch.get());
        // Bounded by the cancel poll interval, nowhere near the query deadline.
        long elapsedMs = System.currentTimeMillis() - cancelledAtMs;
        Assertions.assertTrue(elapsedMs < 5_000, "took " + elapsedMs + "ms to observe cancel");
    }

    // Without a cancel the query deadline is still the bound: the receiver must not wait forever on a
    // silent BE, and it must report TIMEOUT (not some wrapped RPC error) when the deadline passes.
    @Test
    public void testDeadlineEnforcedDespiteBrpcTimeoutWrapping(@Mocked BackendServiceClient client)
            throws Exception {
        mockFetchData(client, BrpcLikeFuture.neverAnswers());

        final int queryTimeoutMs = 1_500;
        ResultReceiver receiver = new ResultReceiver(new TUniqueId(1L, 2L), BACKEND_ID, ADDRESS, queryTimeoutMs);
        Status status = new Status();

        long startMs = System.currentTimeMillis();
        RowBatch batch = receiver.getNext(status);
        long elapsedMs = System.currentTimeMillis() - startMs;

        Assertions.assertTrue(status.isTimeout(), "status after deadline: " + status);
        Assertions.assertNotNull(batch);
        Assertions.assertTrue(elapsedMs >= queryTimeoutMs - 100, "returned before the deadline: " + elapsedMs);
        Assertions.assertTrue(elapsedMs < queryTimeoutMs + 5_000, "returned long after the deadline: " + elapsedMs);
    }

    // Regression guard for the earlier slicing bug: a query whose first fetch_data slices time out but then
    // succeeds must NOT be reported as a query timeout. The bug was catching only the standard
    // TimeoutException, so jprotobuf's ExecutionException("...time out...") escaped to the outer handler and
    // any query taking longer than one slice failed with "Query reached its timeout".
    @Test
    public void testSlowFetchThatEventuallySucceedsIsNotAFalseTimeout(@Mocked BackendServiceClient client)
            throws Exception {
        // Three slice timeouts, then the result arrives — well within the deadline.
        mockFetchData(client, BrpcLikeFuture.answersAfter(3, okEosResult()));

        final int queryTimeoutMs = 600_000;
        ResultReceiver receiver = new ResultReceiver(new TUniqueId(1L, 2L), BACKEND_ID, ADDRESS, queryTimeoutMs);
        Status status = new Status();

        RowBatch batch = receiver.getNext(status);

        Assertions.assertFalse(status.isTimeout(), "must not be a false timeout: " + status);
        Assertions.assertFalse(status.isCancelled(), "must not be cancelled: " + status);
        Assertions.assertTrue(status.ok(), "status should be OK: " + status);
        Assertions.assertNotNull(batch);
    }
}
