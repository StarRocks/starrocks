// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.lake.vector;

import com.starrocks.proto.BuildVectorIndexRequest;
import com.starrocks.proto.BuildVectorIndexResponse;
import com.starrocks.proto.StatusPB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatusCode;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class VectorIndexBuildTaskTest {

    private static ComputeNode mockNode() {
        ComputeNode node = Mockito.mock(ComputeNode.class);
        Mockito.when(node.getHost()).thenReturn("127.0.0.1");
        Mockito.when(node.getBrpcPort()).thenReturn(8060);
        return node;
    }

    private static void injectFuture(VectorIndexBuildTask task, Future<BuildVectorIndexResponse> future)
            throws Exception {
        Field f = VectorIndexBuildTask.class.getDeclaredField("future");
        f.setAccessible(true);
        f.set(task, future);
    }

    private static BuildVectorIndexResponse makeResponse(Integer statusCode) {
        BuildVectorIndexResponse response = new BuildVectorIndexResponse();
        if (statusCode != null) {
            StatusPB status = new StatusPB();
            status.statusCode = statusCode;
            status.errorMsgs = Collections.singletonList("err");
            response.status = status;
        }
        return response;
    }

    @Test
    public void testConstructorAndGetters() {
        ComputeNode node = mockNode();
        long before = System.currentTimeMillis();
        VectorIndexBuildTask task = new VectorIndexBuildTask(node, 1001L, 7L, 3L);
        long after = System.currentTimeMillis();

        Assertions.assertEquals(1001L, task.getTabletId());
        Assertions.assertEquals(7L, task.getVersion());
        Assertions.assertSame(node, task.getNode());
        Assertions.assertTrue(task.getStartTimeMs() >= before && task.getStartTimeMs() <= after);
    }

    @Test
    public void testIsDoneBeforeSend() {
        VectorIndexBuildTask task = new VectorIndexBuildTask(mockNode(), 1L, 1L, 0L);
        Assertions.assertFalse(task.isDone());
        Assertions.assertFalse(task.isAlreadyBuilding());
    }

    @Test
    public void testSendRequestDispatchesToLakeService(@Mocked BrpcProxy brpcProxy,
                                                      @Mocked LakeService lakeService) throws Exception {
        CompletableFuture<BuildVectorIndexResponse> future =
                CompletableFuture.completedFuture(makeResponse(0));
        new Expectations() {
            {
                BrpcProxy.getLakeService("127.0.0.1", 8060);
                result = lakeService;
                lakeService.buildVectorIndex((BuildVectorIndexRequest) any);
                result = future;
            }
        };

        VectorIndexBuildTask task = new VectorIndexBuildTask(mockNode(), 2002L, 9L, 4L);
        task.sendRequest();

        Assertions.assertTrue(task.isDone());
        BuildVectorIndexResponse response = task.getResponse();
        Assertions.assertEquals(0, response.status.statusCode);
    }

    @Test
    public void testGetResponseThrowsOnNonZeroStatus() throws Exception {
        VectorIndexBuildTask task = new VectorIndexBuildTask(mockNode(), 3L, 3L, 0L);
        injectFuture(task, CompletableFuture.completedFuture(makeResponse(1)));

        Assertions.assertTrue(task.isDone());
        RuntimeException ex = Assertions.assertThrows(RuntimeException.class, task::getResponse);
        Assertions.assertTrue(ex.getMessage().contains("Build vector index failed"));
    }

    @Test
    public void testIsAlreadyBuildingDetectsResourceBusy() throws Exception {
        VectorIndexBuildTask task = new VectorIndexBuildTask(mockNode(), 4L, 5L, 0L);
        injectFuture(task,
                CompletableFuture.completedFuture(makeResponse(TStatusCode.RESOURCE_BUSY.getValue())));

        Assertions.assertTrue(task.isAlreadyBuilding());
    }

    @Test
    public void testIsAlreadyBuildingFalseForOkAndOtherErrors() throws Exception {
        VectorIndexBuildTask ok = new VectorIndexBuildTask(mockNode(), 5L, 5L, 0L);
        injectFuture(ok, CompletableFuture.completedFuture(makeResponse(0)));
        Assertions.assertFalse(ok.isAlreadyBuilding());

        VectorIndexBuildTask other = new VectorIndexBuildTask(mockNode(), 6L, 5L, 0L);
        injectFuture(other, CompletableFuture.completedFuture(makeResponse(1)));
        Assertions.assertFalse(other.isAlreadyBuilding());
    }

    @Test
    public void testIsAlreadyBuildingFalseWhenFutureThrows() throws Exception {
        VectorIndexBuildTask task = new VectorIndexBuildTask(mockNode(), 7L, 5L, 0L);
        CompletableFuture<BuildVectorIndexResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("rpc blew up"));
        injectFuture(task, failed);

        // Future is done (completed exceptionally) but get() throws — should return false, not propagate.
        Assertions.assertTrue(task.isDone());
        Assertions.assertFalse(task.isAlreadyBuilding());
    }

    @Test
    public void testIsAlreadyBuildingFalseWhenFutureNotDone() throws Exception {
        VectorIndexBuildTask task = new VectorIndexBuildTask(mockNode(), 8L, 5L, 0L);
        injectFuture(task, new CompletableFuture<>()); // never completes
        Assertions.assertFalse(task.isDone());
        Assertions.assertFalse(task.isAlreadyBuilding());
    }
}
