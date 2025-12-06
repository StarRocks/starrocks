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

import com.google.common.collect.ImmutableList;
import com.starrocks.proto.PCancelPlanFragmentRequest;
import com.starrocks.proto.PCancelPlanFragmentResult;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.MockedBackend;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BackendServiceClientTest {

    @Test
    public void testSendPlanFragmentRetriesOnNoSuchElementSuccess() throws Exception {
        NoSuchElementException noSuchElementException = new NoSuchElementException("inject fail");
        RuntimeException runtimeException = new RuntimeException(noSuchElementException);
        List<RuntimeException> exceptions = ImmutableList.of(noSuchElementException, runtimeException);

        for (RuntimeException ex : exceptions) {
            RecordingBackendService service = mockBackendService(1, 0, ex);
            BackendServiceClient client = BackendServiceClient.getInstance();
            client.execPlanFragmentAsync(new TNetworkAddress("127.0.0.1", 8080), new byte[0], "thrift");
            Assertions.assertEquals(2, service.execPlanCalls.get());
        }
    }

    @Test
    public void testSendPlanFragmentRetriesOnNoSuchElementFail() throws Exception {
        NoSuchElementException noSuchElementException = new NoSuchElementException("inject fail");
        RuntimeException runtimeException = new RuntimeException(noSuchElementException);
        List<RuntimeException> exceptions = ImmutableList.of(noSuchElementException, runtimeException);

        for (RuntimeException ex : exceptions) {
            mockBackendService(10, 0, ex);
            BackendServiceClient client = BackendServiceClient.getInstance();
            assertThatThrownBy(() -> client.execPlanFragmentAsync(new TNetworkAddress("127.0.0.1", 8080), new byte[0], "thrift"))
                    .isInstanceOf(RpcException.class)
                    .hasMessageContaining("inject fail");
        }
    }

    @Test
    public void testCancelPlanFragmentRetriesOnNoSuchElementSuccess() throws Exception {
        NoSuchElementException noSuchElementException = new NoSuchElementException("inject fail");
        RuntimeException runtimeException = new RuntimeException(noSuchElementException);
        List<RuntimeException> exceptions = ImmutableList.of(noSuchElementException, runtimeException);

        for (RuntimeException ex : exceptions) {
            RecordingBackendService service = mockBackendService(0, 1, ex);
            BackendServiceClient client = BackendServiceClient.getInstance();
            client.cancelPlanFragmentAsync(
                    new TNetworkAddress("127.0.0.1", 8081),
                    new TUniqueId(1, 2),
                    new TUniqueId(3, 4),
                    PPlanFragmentCancelReason.INTERNAL_ERROR,
                    true);
            Assertions.assertEquals(2, service.cancelPlanCalls.get());
        }
    }

    @Test
    public void testCancelPlanFragmentRetriesOnNoSuchElementFail() throws Exception {
        NoSuchElementException noSuchElementException = new NoSuchElementException("inject fail");
        RuntimeException runtimeException = new RuntimeException(noSuchElementException);
        List<RuntimeException> exceptions = ImmutableList.of(noSuchElementException, runtimeException);

        for (RuntimeException ex : exceptions) {
            mockBackendService(0, 10, ex);
            BackendServiceClient client = BackendServiceClient.getInstance();
            assertThatThrownBy(() -> client.cancelPlanFragmentAsync(
                    new TNetworkAddress("127.0.0.1", 8081),
                    new TUniqueId(1, 2),
                    new TUniqueId(3, 4),
                    PPlanFragmentCancelReason.INTERNAL_ERROR,
                    true))
                    .isInstanceOf(RpcException.class)
                    .hasMessageContaining("inject fail");
        }
    }

    private RecordingBackendService mockBackendService(int execPlanFailTimes, int cancelPlanFailTimes,
                                                       RuntimeException exception) {
        RecordingBackendService service = new RecordingBackendService(execPlanFailTimes, cancelPlanFailTimes, exception);
        new MockUp<BrpcProxy>() {
            @Mock
            public PBackendService getBackendService(TNetworkAddress address) {
                return service;
            }
        };
        return service;
    }

    private static class RecordingBackendService extends MockedBackend.MockPBackendService {
        private final AtomicInteger execPlanCalls = new AtomicInteger();
        private final AtomicInteger cancelPlanCalls = new AtomicInteger();
        private final int execPlanFailTimes;
        private final int cancelPlanFailTimes;
        private final RuntimeException exception;

        RecordingBackendService(int execPlanFailTimes, int cancelPlanFailTimes, RuntimeException exception) {
            this.execPlanFailTimes = execPlanFailTimes;
            this.cancelPlanFailTimes = cancelPlanFailTimes;
            this.exception = exception;
        }

        @Override
        public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
            if (execPlanCalls.getAndIncrement() < execPlanFailTimes) {
                throw exception;
            }
            return CompletableFuture.completedFuture(new PExecPlanFragmentResult());
        }

        @Override
        public Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(PCancelPlanFragmentRequest request) {
            if (cancelPlanCalls.getAndIncrement() < cancelPlanFailTimes) {
                throw exception;
            }
            return CompletableFuture.completedFuture(new PCancelPlanFragmentResult());
        }
    }
}
