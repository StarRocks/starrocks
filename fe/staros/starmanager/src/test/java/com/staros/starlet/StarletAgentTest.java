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

package com.staros.starlet;

import com.staros.exception.WorkerNotHealthyStarException;
import com.staros.proto.AddShardRequest;
import com.staros.proto.AddShardResponse;
import com.staros.proto.RemoveShardRequest;
import com.staros.proto.RemoveShardResponse;
import com.staros.proto.StarStatus;
import com.staros.proto.StarletGrpc;
import com.staros.proto.StatusCode;
import com.staros.worker.Worker;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class StarletAgentTest {

    private StarletAgent starletAgent;

    @Injectable
    private Worker mockWorker;

    @Mocked
    private StarletGrpc.StarletBlockingStub mockStub;

    @Before
    public void setUp() {
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.STARLET_AGENT;
        starletAgent = StarletAgentFactory.newStarletAgent();
        starletAgent.setWorker(mockWorker);

        new Expectations(starletAgent) {
            {
                starletAgent.prepareBlockingStub();
                result = mockStub;
            }
        };
        new Expectations(mockWorker) {
            {
                mockWorker.getIpPort();
                result = "127.0.0.1:8080";

            }
        };
    }

    @Test
    public void testAddShardSuccess() {
        AddShardRequest request = AddShardRequest.newBuilder().build();
        AddShardResponse response = AddShardResponse.newBuilder()
                .setStatus(StarStatus.newBuilder().setStatusCode(StatusCode.OK).build())
                .build();
        new Expectations(mockStub) {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).addShard(request);
                result = response;
            }
        };

        try {
            // the add shard request is succeed, with an OK status code
            starletAgent.addShard(request);
        } catch (Exception e) {
            Assert.fail("add shard should not throw exception");
        }
        new Verifications() {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).addShard(request);
                times = 1;
            }
        };
    }

    @Test
    public void testAddShardFailure() {
        AddShardRequest request = AddShardRequest.newBuilder().build();
        AddShardResponse response = AddShardResponse.newBuilder()
                .setStatus(StarStatus.newBuilder().setStatusCode(StatusCode.INTERNAL).build())
                .build();
        new Expectations(mockStub) {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).addShard(request);
                result = response;
            }
        };

        // the add shard request is failed, with a non-OK status code
        Assert.assertThrows(WorkerNotHealthyStarException.class, () -> starletAgent.addShard(request));
        new Verifications() {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).addShard(request);
                times = 1;
            }
        };
    }

    @Test
    public void testAddShardRetryFailureWithNonRetryableError() {
        AddShardRequest request = AddShardRequest.newBuilder().build();
        new Expectations(mockStub) {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).addShard(request);
                result = new StatusRuntimeException(Status.Code.ABORTED.toStatus());
            }
        };
        // ABORTED is not a retryable GRPC exception, should fail fast
        Assert.assertThrows(WorkerNotHealthyStarException.class, () -> starletAgent.addShard(request));
        new Verifications() {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).addShard(request);
                times = 1;
            }
        };
    }

    @Test
    public void testAddShardRetryFailureAfterMaxRetryTimes() {
        AddShardRequest request = AddShardRequest.newBuilder().build();

        // DEADLINE_EXCEEDED is a retryable GRPC exception
        new Expectations(mockStub) {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).addShard(request);
                result = new StatusRuntimeException(Status.Code.DEADLINE_EXCEEDED.toStatus());
            }
        };
        // reached max retry times 3
        Assert.assertThrows(WorkerNotHealthyStarException.class, () -> starletAgent.addShard(request));
        new Verifications() {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).addShard(request);
                times = 3;
            }
        };

        // UNAVAILABLE is a retryable GRPC exception
        new Expectations(mockStub) {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).addShard(request);
                result = new StatusRuntimeException(Status.Code.UNAVAILABLE.toStatus());
            }
        };
        // reached max retry times 3
        Assert.assertThrows(WorkerNotHealthyStarException.class, () -> starletAgent.addShard(request));
        new Verifications() {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).addShard(request);
                times = 3;
            }
        };
    }

    @Test
    public void testRemoveShardSuccess() {
        RemoveShardRequest request = RemoveShardRequest.newBuilder().build();
        RemoveShardResponse response = RemoveShardResponse.newBuilder()
                .setStatus(StarStatus.newBuilder().setStatusCode(StatusCode.OK).build())
                .build();
        new Expectations(mockStub) {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).removeShard(request);
                result = response;
            }
        };
        try {
            // the remove shard request is succeed, with an OK status code
            starletAgent.removeShard(request);
        } catch (Exception e) {
            Assert.fail("remove shard should not throw exception");
        }
        new Verifications() {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).removeShard(request);
                times = 1;
            }
        };
    }

    @Test
    public void testRemoveShardFailure() {
        RemoveShardRequest request = RemoveShardRequest.newBuilder().build();
        RemoveShardResponse response = RemoveShardResponse.newBuilder()
                .setStatus(StarStatus.newBuilder().setStatusCode(StatusCode.INTERNAL).build())
                .build();
        new Expectations(mockStub) {
            {
                starletAgent.prepareBlockingStub();
                result = mockStub;

                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).removeShard(request);
                result = response;
            }
        };
        // the remove shard request is succeed, but with a non-OK status code
        Assert.assertThrows(WorkerNotHealthyStarException.class, () -> starletAgent.removeShard(request));
        new Verifications() {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).removeShard(request);
                times = 1;
            }
        };
    }

    @Test
    public void testRemoveShardRetryFailureWithNonRetryableError() {
        RemoveShardRequest request = RemoveShardRequest.newBuilder().build();
        new Expectations(mockStub) {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).removeShard(request);
                result = new StatusRuntimeException(Status.Code.ABORTED.toStatus());
            }
        };
        Assert.assertThrows(WorkerNotHealthyStarException.class, () -> starletAgent.removeShard(request));
        // ABORTED is not a retryable GRPC exception, should fail fast
        new Verifications() {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).removeShard(request);
                times = 1;
            }
        };
    }

    @Test
    public void testRemoveShardRetryFailureAfterMaxRetryTimes() {
        // DEADLINE_EXCEEDED is a retryable GRPC exception
        RemoveShardRequest request = RemoveShardRequest.newBuilder().build();
        new Expectations(mockStub) {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).removeShard(request);
                result = new StatusRuntimeException(Status.Code.DEADLINE_EXCEEDED.toStatus());
            }
        };
        // reached max retry times 3
        Assert.assertThrows(WorkerNotHealthyStarException.class, () -> starletAgent.removeShard(request));
        new Verifications() {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).removeShard(request);
                times = 3;
            }
        };

        // UNAVAILABLE is a retryable GRPC exception
        new Expectations(mockStub) {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).removeShard(request);
                result = new StatusRuntimeException(Status.Code.UNAVAILABLE.toStatus());
            }
        };
        // reached max retry times 3
        Assert.assertThrows(WorkerNotHealthyStarException.class, () -> starletAgent.removeShard(request));
        new Verifications() {
            {
                mockStub.withDeadlineAfter(anyLong, (TimeUnit) any).removeShard(request);
                times = 3;
            }
        };
    }
}
