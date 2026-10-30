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

package com.starrocks.rpc;

import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PFetchDataResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The brpc client hands back a plain {@link CompletableFuture}; these tests pin the contract the FE's
 * callers were written against, which differs from it in how a timed wait ends.
 */
public class LegacyTimeoutFutureTest {

    @Test
    public void testZeroTimeoutWaitsForTheResponse() throws Exception {
        CompletableFuture<String> raw = new CompletableFuture<>();
        Future<String> future = LegacyTimeoutFuture.wrap(raw);
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            raw.complete("ok");
        });
        // A plain CompletableFuture throws TimeoutException here at once.
        Assertions.assertEquals("ok", future.get(0, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testNegativeTimeoutWaitsForTheResponse() throws Exception {
        CompletableFuture<String> raw = new CompletableFuture<>();
        Future<String> future = LegacyTimeoutFuture.wrap(raw);
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            raw.complete("ok");
        });
        Assertions.assertEquals("ok", future.get(-1, TimeUnit.SECONDS));
    }

    @Test
    public void testElapsedTimeoutIsAnExecutionException() {
        Future<String> future = LegacyTimeoutFuture.wrap(new CompletableFuture<>());
        ExecutionException e = Assertions.assertThrows(ExecutionException.class,
                () -> future.get(10, TimeUnit.MILLISECONDS));
        Assertions.assertInstanceOf(TimeoutException.class, e.getCause());
        // ResultReceiver reports a query timeout, rather than black-listing the backend, only on this marker.
        Assertions.assertTrue(e.getMessage().contains("time out"), e.getMessage());
    }

    @Test
    public void testCompletedFutureReturnsItsValueAndFailureUnchanged() throws Exception {
        Assertions.assertEquals("ok",
                LegacyTimeoutFuture.wrap(CompletableFuture.completedFuture("ok")).get(1, TimeUnit.MILLISECONDS));

        RuntimeException cause = new RuntimeException("rpc failed");
        Future<String> failed = LegacyTimeoutFuture.wrap(CompletableFuture.failedFuture(cause));
        ExecutionException e = Assertions.assertThrows(ExecutionException.class,
                () -> failed.get(1, TimeUnit.SECONDS));
        Assertions.assertSame(cause, e.getCause());
    }

    @Test
    public void testCancelDoesNotAbandonTheResponse() throws Exception {
        CompletableFuture<String> raw = new CompletableFuture<>();
        Future<String> future = LegacyTimeoutFuture.wrap(raw);
        Assertions.assertFalse(future.cancel(true));
        Assertions.assertFalse(future.isCancelled());
        Assertions.assertFalse(raw.isCancelled());
        raw.complete("ok");
        Assertions.assertTrue(future.isDone());
        Assertions.assertEquals("ok", future.get());
    }

    @Test
    public void testWrapServiceWrapsEveryReturnedFuture() throws Exception {
        CompletableFuture<PExecPlanFragmentResult> deploy = new CompletableFuture<>();
        PFetchDataResult fetched = new PFetchDataResult();
        PBackendService raw = (PBackendService) Proxy.newProxyInstance(PBackendService.class.getClassLoader(),
                new Class<?>[] {PBackendService.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "execPlanFragmentAsync":
                            return deploy;
                        case "fetchDataAsync":
                            return CompletableFuture.completedFuture(fetched);
                        case "cancelPlanFragmentAsync":
                            throw new IllegalStateException("no connection");
                        default:
                            return null;
                    }
                });

        PBackendService service = LegacyTimeoutFuture.wrapService(raw, PBackendService.class);

        Future<PExecPlanFragmentResult> deployFuture = service.execPlanFragmentAsync(null);
        Assertions.assertInstanceOf(LegacyTimeoutFuture.class, deployFuture);
        // The deploy wait of a job whose query_delivery_timeout is unset: it must not give up.
        Assertions.assertFalse(deployFuture.isDone());
        CompletableFuture.runAsync(() -> deploy.complete(new PExecPlanFragmentResult()));
        Assertions.assertNotNull(deployFuture.get(0, TimeUnit.MILLISECONDS));

        Assertions.assertSame(fetched, service.fetchDataAsync(null).get(1, TimeUnit.SECONDS));
        Assertions.assertNull(service.getInfo(null));

        // An exception thrown while sending reaches the caller as itself, not as a reflection wrapper.
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> service.cancelPlanFragmentAsync(null));
        Assertions.assertEquals("no connection", e.getMessage());
    }
}
