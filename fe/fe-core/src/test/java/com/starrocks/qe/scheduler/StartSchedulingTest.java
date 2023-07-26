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

package com.starrocks.qe.scheduler;

import com.starrocks.common.Reference;
import com.starrocks.common.UserException;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.rpc.PExecPlanFragmentRequest;
import com.starrocks.rpc.RpcException;
import com.starrocks.thrift.FrontendServiceVersion;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.starrocks.utframe.MockedBackend.MockPBackendService;

public class StartSchedulingTest extends SchedulerTestBase {
    private boolean originalEnableProfile;

    @Before
    public void before() {
        originalEnableProfile = connectContext.getSessionVariable().isEnableProfile();
    }

    @After
    public void after() {
        connectContext.getSessionVariable().setEnableProfile(originalEnableProfile);
    }

    @Test
    public void testDeploySuccess() throws Exception {
        setBackendService(new MockPBackendService());

        String sql = "select count(1) from lineitem";
        DefaultCoordinator scheduler = startScheduling(sql);

        Assert.assertTrue(scheduler.getExecStatus().ok());
    }

    @Test
    public void testDeployReturnErrorStatus() {
        setBackendService(new MockPBackendService() {
            @Override
            public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
                return submit(() -> {
                    PExecPlanFragmentResult result = new PExecPlanFragmentResult();
                    StatusPB pStatus = new StatusPB();
                    pStatus.statusCode = TStatusCode.INTERNAL_ERROR.getValue();
                    pStatus.errorMsgs = Collections.singletonList("test error message");
                    result.status = pStatus;
                    return result;
                });
            }
        });

        String sql = "select count(1) from lineitem";
        Assert.assertThrows("test error message", UserException.class, () -> startScheduling(sql));
    }

    @Test
    public void testDeployFutureThrowException() throws Exception {
        connectContext.getSessionVariable().setEnableProfile(true);

        Reference<Future<PExecPlanFragmentResult>> deployFuture = new Reference<>();
        setBackendService(address -> {
            if (!backend3.getHost().equals(address.getHostname())) {
                return new MockPBackendService();
            }
            return new MockPBackendService() {
                @Override
                public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
                    return deployFuture.getRef();
                }
            };
        });

        String sql = "select count(1) from lineitem t1 JOIN [shuffle] lineitem t2 using(l_orderkey)";

        deployFuture.setRef(
                mockFutureWithException(new ExecutionException("test execution exception", new Exception())));
        Assert.assertThrows("test execution exception", RpcException.class, () -> startScheduling(sql));
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !SimpleScheduler.isInBlacklist(backend3.getId()));

        deployFuture.setRef(mockFutureWithException(new InterruptedException("test interrupted exception")));
        DefaultCoordinator scheduler = getScheduler(sql);
        Assert.assertThrows("test interrupted exception", UserException.class, () -> scheduler.startScheduling());

        // The deployed executions haven't reported.
        Assert.assertFalse(scheduler.isDone());

        // Shouldn't deploy the rest instances, when the previous instance deployment failed.
        Assert.assertTrue(scheduler.getBackendNums().size() < scheduler.getInstanceIds().size());
        // Receive execution reports.
        scheduler.getBackendExecutions().forEach(execution -> {
            TReportExecStatusParams request = new TReportExecStatusParams(FrontendServiceVersion.V1);
            request.setBackend_num(execution.getIndexInJob());
            request.setDone(true);
            request.setStatus(new TStatus(TStatusCode.CANCELLED));
            request.setFragment_instance_id(execution.getInstanceId());

            scheduler.updateFragmentExecStatus(request);
        });
        Assert.assertTrue(scheduler.isDone());
    }

    @Test
    public void testDeployThrowException() {
        setBackendService(address -> {
            if (!backend3.getHost().equals(address.getHostname())) {
                return new MockPBackendService();
            }
            return new MockPBackendService() {
                @Override
                public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
                    throw new RuntimeException("test runtime exception");
                }
            };
        });

        String sql = "select count(1) from lineitem";

        Assert.assertThrows("test runtime exception", RpcException.class, () -> startScheduling(sql));
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !SimpleScheduler.isInBlacklist(backend3.getId()));
    }

    @Test
    public void testDeployTimeout() throws Exception {
        int prevQueryDeliveryTimeoutSecond = connectContext.getSessionVariable().getQueryDeliveryTimeoutS();

        try {
            connectContext.getSessionVariable().setQueryDeliveryTimeoutS(1);

            setBackendService(new MockPBackendService() {
                @Override
                public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
                    return submit(() -> {
                        try {
                            Thread.sleep(5_000L);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }

                        PExecPlanFragmentResult result = new PExecPlanFragmentResult();
                        StatusPB pStatus = new StatusPB();
                        pStatus.statusCode = 0;
                        result.status = pStatus;
                        return result;
                    });
                }
            });

            String sql = "select count(1) from lineitem t1 JOIN [shuffle] lineitem t2 using(l_orderkey)";
            DefaultCoordinator scheduler = getScheduler(sql);
            Assert.assertThrows("deploy query timeout", UserException.class, () -> scheduler.startScheduling());
        } finally {
            connectContext.getSessionVariable().setQueryDeliveryTimeoutS(prevQueryDeliveryTimeoutSecond);
        }
    }

    private static Future<PExecPlanFragmentResult> mockFutureWithException(Exception exception) {

        return new Future<PExecPlanFragmentResult>() {

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
                return false;
            }

            @Override
            public PExecPlanFragmentResult get() {
                return null;
            }

            @Override
            public PExecPlanFragmentResult get(long timeout, @NotNull TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                if (exception instanceof InterruptedException) {
                    throw (InterruptedException) exception;
                } else if (exception instanceof ExecutionException) {
                    throw (ExecutionException) exception;
                } else if (exception instanceof TimeoutException) {
                    throw (TimeoutException) exception;
                } else {
                    throw new IllegalArgumentException("mockFutureWithException with illegal exception: " + exception);
                }
            }
        };
    }

}
