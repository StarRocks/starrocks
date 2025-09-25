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

import com.google.common.annotations.VisibleForTesting;
import com.staros.exception.StarException;
import com.staros.exception.WorkerNotHealthyStarException;
import com.staros.proto.AddShardRequest;
import com.staros.proto.AddShardResponse;
import com.staros.proto.RemoveShardRequest;
import com.staros.proto.RemoveShardResponse;
import com.staros.proto.StarStatus;
import com.staros.proto.StarletGrpc;
import com.staros.proto.StarletHeartbeatRequest;
import com.staros.proto.StarletHeartbeatResponse;
import com.staros.proto.StatusCode;
import com.staros.proto.WorkerGroupProperty;
import com.staros.proto.WorkerState;
import com.staros.util.Config;
import com.staros.util.StatusFactory;
import com.staros.worker.Worker;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class StarletAgent {
    private static final Logger LOG = LogManager.getLogger(StarletAgent.class);
    private static final int GRPC_MAX_RETRY_TIMES = 3;

    private final String starMgrAddress;
    // the blockingStub to be used for grpc call, the object in the reference can be null in case GRPC error.
    private AtomicReference<StarletGrpc.StarletBlockingStub> blockingStub;
    private ManagedChannel channel;
    private Worker worker;
    private int heartbeatRetryCount;

    public StarletAgent() {
        starMgrAddress = String.format("%s:%s", Config.STARMGR_IP, Config.STARMGR_RPC_PORT);
        blockingStub = new AtomicReference<>(null);
        channel = null;
        worker = null;
        heartbeatRetryCount = 0;
    }

    public void setWorker(Worker w) {
        worker = w;
    }

    public void disconnectWorker() {
        if (channel != null) {
            channel.shutdown();
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (Exception exception) {
                LOG.info("Got exception while waiting channel shutdown.", exception);
            }
            channel = null;
        }
    }

    /*
     * @return Pair<success, stateChanged>
     */
    public Pair<Boolean, Boolean> heartbeat() {
        StarStatus status = null;
        try {
            StarletGrpc.StarletBlockingStub stub = prepareBlockingStub();

            WorkerGroupProperty property = WorkerGroupProperty.newBuilder()
                    .setReplicationType(worker.getReplicationType())
                    .setWarmupLevel(worker.getWarmupLevel())
                    .build();
            StarletHeartbeatRequest request = StarletHeartbeatRequest.newBuilder()
                    .setStarMgrLeader(starMgrAddress)
                    .setServiceId(worker.getServiceId())
                    .setWorkerGroupId(worker.getGroupId())
                    .setWorkerId(worker.getWorkerId())
                    .setWorkerGroupProperty(property)
                    .build();
            StarletHeartbeatResponse response = stub
                    .withDeadlineAfter(Config.WORKER_HEARTBEAT_GRPC_RPC_TIME_OUT_SEC, TimeUnit.SECONDS)
                    .starletHeartbeat(request);
            status = response.getStatus();
        } catch (StatusRuntimeException exception) {
            // GRPC related exception
            LOG.warn("caught GRPC exception when sending heartbeat to worker {}, {}.", worker.getIpPort(), exception);
            status = handleGrpcException(exception);
        } catch (Exception e) {
            // Other generic exception
            LOG.warn("caught generic exception when sending heartbeat to worker {}, {}.", worker.getIpPort(), e);
            status = StatusFactory.getStatus(StatusCode.INTERNAL, e.getMessage());
        }

        boolean changed = false;
        if (status.getStatusCode() != StatusCode.OK) {
            if (status.getStatusCode() == StatusCode.SHUT_DOWN) {
                worker.setState(WorkerState.SHUTTING_DOWN);
                heartbeatRetryCount = 0;
                changed = true;
            } else {
                if (heartbeatRetryCount < Config.WORKER_HEARTBEAT_RETRY_COUNT) {
                    heartbeatRetryCount++;
                } else {
                    changed = worker.setState(WorkerState.DOWN);
                }
            }
            LOG.warn("sending heartbeat to worker {} failed, {}:{}.",
                    worker.getIpPort(), status.getStatusCode(), status.getErrorMsg());
            return Pair.of(false, changed);
        } else {
            LOG.debug("sending heartbeat to worker {} succeed.", worker.getIpPort());

            changed = worker.setState(WorkerState.ON);
            heartbeatRetryCount = 0;
            return Pair.of(true, changed);
        }
    }

    public void addShard(AddShardRequest request) throws StarException {
        int retryCount = 1;
        while (retryCount <= GRPC_MAX_RETRY_TIMES) {
            try {
                StarletGrpc.StarletBlockingStub stub = prepareBlockingStub();
                AddShardResponse response = stub
                        .withDeadlineAfter(Config.GRPC_RPC_TIME_OUT_SEC, TimeUnit.SECONDS)
                        .addShard(request);
                StarStatus status = response.getStatus();
                if (status.getStatusCode() != StatusCode.OK) {
                    LOG.warn("add shard to worker {} failed, {}:{}.", worker.getIpPort(), status.getStatusCode(),
                            status.getErrorMsg());
                    throw new WorkerNotHealthyStarException("failed to add shard to worker {}", worker.getIpPort());
                }
                LOG.debug("add shard to worker {} succeed.", worker.getIpPort());
                // no need to retry
                return;
            } catch (StatusRuntimeException e) {
                // GRPC related exception, not retryable, throw exception
                if (!isGrpcExceptionRetryable(e) || retryCount >= GRPC_MAX_RETRY_TIMES) {
                    LOG.warn("caught GRPC exception when adding shard to worker {}, {}.",
                            worker.getIpPort(), e.getMessage());
                    throw new WorkerNotHealthyStarException("failed to add shard to worker {}", worker.getIpPort());
                }
                LOG.debug("caught GRPC exception when adding shard to worker {}, retry {} times", worker.getIpPort(),
                        retryCount);
                retryCount++;
            } catch (Exception e) {
                // Other generic exception
                LOG.warn("caught exception when adding shard to worker {}", worker.getIpPort(), e);
                throw new WorkerNotHealthyStarException("failed to add shard to worker {}", worker.getIpPort());
            }
        }
    }

    public void removeShard(RemoveShardRequest request) throws StarException {
        int retryCount = 1;
        while (retryCount <= GRPC_MAX_RETRY_TIMES) {
            try {
                StarletGrpc.StarletBlockingStub stub = prepareBlockingStub();
                RemoveShardResponse response = stub
                        .withDeadlineAfter(Config.GRPC_RPC_TIME_OUT_SEC, TimeUnit.SECONDS)
                        .removeShard(request);
                StarStatus status = response.getStatus();
                if (status.getStatusCode() != StatusCode.OK) {
                    LOG.warn("remove shard from worker {} failed, {}:{}.", worker.getIpPort(), status.getStatusCode(),
                            status.getErrorMsg());
                    throw new WorkerNotHealthyStarException("failed to remove shard from worker {}", worker.getIpPort());
                }
                LOG.debug("remove shard from worker {} succeed.", worker.getIpPort());
                // no need to retry
                return;
            } catch (StatusRuntimeException e) {
                // GRPC related exception, not retryable, throw exception
                if (!isGrpcExceptionRetryable(e) || retryCount >= GRPC_MAX_RETRY_TIMES) {
                    LOG.warn("caught GRPC exception when removing shard from worker {}, {}.",
                            worker.getIpPort(), e.getMessage());
                    throw new WorkerNotHealthyStarException("failed to remove shard from worker {}", worker.getIpPort());
                }
                LOG.debug("caught GRPC exception when removing shard from worker {}, retry {} times",
                        worker.getIpPort(), retryCount);
                retryCount++;
            } catch (Exception e) {
                // Other generic exception
                LOG.warn("caught exception when removing shard from worker {}", worker.getIpPort(), e);
                throw new WorkerNotHealthyStarException("failed to remove shard from worker {}", worker.getIpPort());
            }
        }
    }

    private boolean isGrpcExceptionRetryable(StatusRuntimeException exception) {
        return exception.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED ||
                exception.getStatus().getCode() == Status.Code.UNAVAILABLE;
    }

    private StarStatus handleGrpcException(StatusRuntimeException exception) {
        switch (exception.getStatus().getCode()) {
            case ABORTED:
            case CANCELLED:
            case DEADLINE_EXCEEDED:
            case INTERNAL:
            case RESOURCE_EXHAUSTED:
            case UNAVAILABLE:
            case UNIMPLEMENTED:
            case UNKNOWN:
                // reset the stub but not the channel, so the stub will be rebuilt as necessary when needed
                // and the channel can be cleaned as well.
                blockingStub.set(null);
                break;
            default:
                break;
        }
        return StatusFactory.getStatus(StatusCode.GRPC, exception.getMessage());
    }

    @VisibleForTesting
    protected StarletGrpc.StarletBlockingStub prepareBlockingStub() {
        StarletGrpc.StarletBlockingStub stub = blockingStub.get();
        if (stub == null) {
            return buildGrpcChannelAndStub();
        } else {
            return stub;
        }
    }

    private StarletGrpc.StarletBlockingStub buildGrpcChannelAndStub() {
        ManagedChannel oldChannel = channel;
        // FIXME: build channel and stub, workaround grpc ExponentialBackoffPolicy issue, workaround it by reset the stub
        //  and underlying channel
        channel = ManagedChannelBuilder.forTarget(worker.getIpPort())
                .maxInboundMessageSize(Config.GRPC_CHANNEL_MAX_MESSAGE_SIZE)
                .usePlaintext()
                .build();
        StarletGrpc.StarletBlockingStub stub = StarletGrpc.newBlockingStub(channel);
        blockingStub.set(stub);
        if (oldChannel != null) {
            oldChannel.shutdown();
        }
        return stub;
    }
}
