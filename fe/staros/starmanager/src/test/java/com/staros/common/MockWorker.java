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


package com.staros.common;

import com.staros.proto.AddShardInfo;
import com.staros.proto.AddShardRequest;
import com.staros.proto.AddShardResponse;
import com.staros.proto.RemoveShardRequest;
import com.staros.proto.RemoveShardResponse;
import com.staros.proto.StarManagerGrpc;
import com.staros.proto.StarStatus;
import com.staros.proto.StarletGrpc;
import com.staros.proto.StarletHeartbeatRequest;
import com.staros.proto.StarletHeartbeatResponse;
import com.staros.proto.StatusCode;
import com.staros.proto.WorkerHeartbeatRequest;
import com.staros.util.Config;
import com.staros.util.LockCloseable;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class MockWorker extends StarletGrpc.StarletImplBase {
    private static final Logger LOG = LogManager.getLogger(MockWorker.class);
    private String starmgrLeader;
    private String serviceId;
    private long workerGroupId;
    private long workerId;
    private long startTime;
    private Map<Long, AddShardInfo> shards; // <shardId, AddShardInfo>

    private AtomicBoolean running;
    private Thread background;

    private ReentrantLock lock;

    private Server server;

    private int heartbeatCount;
    private boolean disableHeartbeat; // disable worker heartbeat
    private StatusCode errorCode; // the error code to ingest
    private boolean responding = true; // whether responds to RPC request

    private class MockWorkerService extends StarletGrpc.StarletImplBase {
        private MockWorker worker;

        @Override
        public void addShard(AddShardRequest request, StreamObserver<AddShardResponse> responseObserver) {
            try (LockCloseable lk = new LockCloseable(worker.lock)) {
                AddShardResponse.Builder response = AddShardResponse.newBuilder();
                if (errorCode != StatusCode.OK) {
                    StarStatus status = StarStatus.newBuilder().setStatusCode(errorCode).build();
                    response.setStatus(status);
                } else {
                    List<AddShardInfo> infos = request.getShardInfoList();
                    for (AddShardInfo info : infos) {
                        worker.shards.put(info.getShardId(), info);
                    }
                }
                responseObserver.onNext(response.build());
                responseObserver.onCompleted();
            }
        }

        @Override
        public void removeShard(RemoveShardRequest request, StreamObserver<RemoveShardResponse> responseObserver) {
            try (LockCloseable lk = new LockCloseable(worker.lock)) {
                RemoveShardResponse.Builder response = RemoveShardResponse.newBuilder();
                if (errorCode != StatusCode.OK) {
                    StarStatus status = StarStatus.newBuilder().setStatusCode(errorCode).build();
                    response.setStatus(status);
                } else {
                    List<Long> shardIds = request.getShardIdsList();
                    for (Long shardId : shardIds) {
                        worker.shards.remove(shardId);
                    }
                }
                responseObserver.onNext(response.build());
                responseObserver.onCompleted();
            }
        }

        @Override
        public void starletHeartbeat(StarletHeartbeatRequest request, StreamObserver<StarletHeartbeatResponse> responseObserver) {
            StarletHeartbeatResponse.Builder response = StarletHeartbeatResponse.newBuilder();
            if (!responding) {
                try {
                    // sleep until timeout
                    Thread.sleep(Config.WORKER_HEARTBEAT_GRPC_RPC_TIME_OUT_SEC * 1000L + 100);
                } catch (InterruptedException e) {
                    // do nothing
                }
                return;
            }

            try (LockCloseable lk = new LockCloseable(worker.lock)) {
                worker.heartbeatCount++;
                if (errorCode != StatusCode.OK) {
                    StarStatus status = StarStatus.newBuilder().setStatusCode(errorCode).build();
                    response.setStatus(status);
                } else {
                    worker.starmgrLeader = request.getStarMgrLeader();
                    worker.serviceId = request.getServiceId();
                    worker.workerGroupId = request.getWorkerGroupId();
                    worker.workerId = request.getWorkerId();
                }
            }

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        public MockWorkerService(MockWorker worker) {
            this.worker = worker;
        }
    }

    public MockWorker() {
        this.running = new AtomicBoolean(false);
        this.lock = new ReentrantLock();

        reset();
    }

    private void reset() {
        try (LockCloseable lk = new LockCloseable(lock)) {
            starmgrLeader = null;
            serviceId = "0";
            workerGroupId = 0;
            workerId = 0;
            startTime = 0;
            shards = new HashMap<>();
            server = null;
            background = null;
            heartbeatCount = 0;
            disableHeartbeat = false;
            errorCode = StatusCode.OK;
            responding = true;
        }
    }

    public void stop() {
        if (!running.compareAndSet(true /* expect */, false /* wanted */)) {
            return;
        }

        try {
            server.shutdown();
            server.awaitTermination();

            background.interrupt();
            background.join();
        } catch (Exception e) {
            LOG.warn("stop mock worker caught exception! {}", e.getMessage());
        }

        reset();
    }

    public void start() {
        if (!running.compareAndSet(false /* expect */, true /* wanted */)) {
            return;
        }

        startTime = System.currentTimeMillis();
        try {
            // let server auto detect available listening port
            server = ServerBuilder.forPort(0).addService(new MockWorkerService(this))
                    .maxInboundMessageSize(Config.GRPC_CHANNEL_MAX_MESSAGE_SIZE)
                    .build().start();
        } catch (IOException e) {
            System.out.println(e);
            System.exit(-1);
        }

        background = new Thread(() -> runBackground());
        background.start();
    }

    void runBackground() {
        while (true) {
            if (!running.get()) {
                break;
            }

            try {
                heartbeat();
            } catch (Exception e) {
                // just ignore
            }

            try {
                Thread.sleep(1 * 1000); // 1 second
            } catch (InterruptedException e) {
                LOG.warn("background thread sleep interrupted! {}", e.getMessage());
            }
        }
    }

    private void heartbeat() {
        ManagedChannel channel = null;
        WorkerHeartbeatRequest request = null;
        String leaderAddress = null;
        try (LockCloseable lk = new LockCloseable(lock)) {
            if (disableHeartbeat) {
                return;
            }

            if (starmgrLeader == null) {
                return;
            }
            leaderAddress = starmgrLeader;
            request = WorkerHeartbeatRequest.newBuilder()
                    .setServiceId(serviceId)
                    .setWorkerId(workerId)
                    .setStartTime(startTime)
                    .addAllShardIds(shards.keySet())
                    .build();
        }

        try {
            channel = ManagedChannelBuilder.forTarget(leaderAddress)
                    .maxInboundMessageSize(Config.GRPC_CHANNEL_MAX_MESSAGE_SIZE)
                    .usePlaintext()
                    .build();
            StarManagerGrpc.StarManagerBlockingStub bStub = StarManagerGrpc.newBlockingStub(channel);
            bStub.workerHeartbeat(request);
        } catch (Exception exception) {
            LOG.warn("Fail to send heartbeat request.", exception);
        } finally {
            if (channel != null) {
                channel.shutdown();
                try {
                    channel.awaitTermination(2, TimeUnit.SECONDS);
                } catch (Exception exception) {
                    // ignores the exception
                }
            }
        }
    }

    public int getShardCount() {
        try (LockCloseable lk = new LockCloseable(lock)) {
            return shards.size();
        }
    }

    public int getHeartbeatCount() {
        try (LockCloseable lk = new LockCloseable(lock)) {
            return heartbeatCount;
        }
    }

    public void setDisableHeartbeat(boolean v) {
        try (LockCloseable lk = new LockCloseable(lock)) {
            disableHeartbeat = v;
        }
    }

    public void setError(StatusCode errCode) {
        try (LockCloseable lk = new LockCloseable(lock)) {
            errorCode = errCode;
        }
    }

    public void setResponding(boolean responding) {
        this.responding = responding;
    }

    public long getWorkerId() {
        try (LockCloseable lk = new LockCloseable(lock)) {
            return workerId;
        }
    }

    /**
     * Simulate worker restart
     */
    public void simulateRestart() {
        try (LockCloseable ignored = new LockCloseable(lock)) {
            startTime = System.currentTimeMillis();
            shards.clear();
        }
    }

    public long getStartTime() {
        return startTime;
    }

    public boolean addShard(long shardId) {
        try (LockCloseable lk = new LockCloseable(lock)) {
            AddShardInfo info = AddShardInfo.newBuilder().setShardId(shardId).build();
            return shards.put(shardId, info) == null;
        }
    }

    public int getRpcPort() {
        return server.getPort();
    }
}
