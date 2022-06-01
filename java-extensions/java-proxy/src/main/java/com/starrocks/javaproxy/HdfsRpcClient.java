// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.javaproxy;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class HdfsRpcClient {

    private static final Logger logger = LogManager.getLogger(HdfsRpcClient.class.getName());

    private final PBackendServiceGrpc.PBackendServiceBlockingStub blockingStub;

    public HdfsRpcClient(Channel channel) {
        blockingStub = PBackendServiceGrpc.newBlockingStub(channel);
    }

    public void test(String path) {
        HdfsRequest request = HdfsRequest.newBuilder().setPath(path).build();
        HdfsResponse resp = blockingStub.hdfsOpen(request);
        String sessionId = resp.getSessionId();
        logger.info(String.format("Session Id = " + sessionId));

        // get size, read, get stats
        {
            request = HdfsRequest.newBuilder().setSessionId(sessionId).setOffset(0).build();
            resp = blockingStub.hdfsGetSize(request);
            logger.info(String.format("size = %d", resp.getSize()));
            long size = resp.getSize();

            long blockSize = 1024 * 1024;
            long offset = 0;
            while (offset < size) {
                blockSize = Math.min(blockSize, size - offset);
                request = HdfsRequest.newBuilder().setSessionId(sessionId).setOffset((int) offset)
                        .setSize((int) blockSize).build();
                resp = blockingStub.hdfsRead(request);
                logger.info(String.format("read bytes = %d", resp.getData().size()));
                offset += blockSize;
            }
            resp = blockingStub.hdfsGetStats(request);
            logger.info(String.format("stats = %d", resp.getStats().getTotalBytesRead()));
        }

        request = HdfsRequest.newBuilder().setSessionId(sessionId).build();
        blockingStub.hdfsClose(request);
    }

    public static void main(String[] args) throws Exception {
        String target = "localhost:50051";
        String path = "file:///Users/dirlt/.ssh/id_rsa.pub";
        if (args.length > 0) {
            path = args[0];
        }
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();
        try {
            HdfsRpcClient client = new HdfsRpcClient(channel);
            client.test(path);
        } finally {
            // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
            // resources the channel should be shut down when it will no longer be used. If it may be used
            // again leave it running.
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
