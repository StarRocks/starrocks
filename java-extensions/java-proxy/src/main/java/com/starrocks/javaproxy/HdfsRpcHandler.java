// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.javaproxy;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class HdfsRpcHandler extends PBackendServiceGrpc.PBackendServiceImplBase {
    private final Logger logger = LogManager.getLogger(HdfsRpcHandler.class.getCanonicalName());
    private final AtomicLong rpcOnFly = new AtomicLong();

    private static class CacheValue {
        public FileSystem fs;
        public Path path;
        public FSDataInputStream inputStream;
        public ByteBuffer buffer;
    }

    private Configuration configuration;
    private Cache<String, CacheValue> cache;

    public HdfsRpcHandler() {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration = conf;

        cache = CacheBuilder.newBuilder()
                .maximumSize(128 * 1024)
                .concurrencyLevel(32)
                .expireAfterAccess(Duration.ofMinutes(5))
                .removalListener(new RemovalListener<String, CacheValue>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, CacheValue> notification) {
                        CacheValue cv = notification.getValue();
                        logger.info(String.format("release session %s", notification.getKey()));
                        cv.buffer.clear();
                        try {
                            cv.inputStream.close();
                        } catch (IOException e) {
                        }
                    }
                })
                .build();
    }

    // ============================================================
    private ByteBuffer newBuffer(int size) {
        int cap = 1024;
        while (cap < size) {
            cap *= 2;
        }
        return ByteBuffer.allocate(cap);
    }

    private ByteBuffer resizeBuffer(ByteBuffer buffer, int size) {
        int cap = buffer.capacity();
        if (cap >= size) {
            return buffer;
        }
        while (cap < size) {
            cap *= 2;
        }
        buffer.clear();
        return ByteBuffer.allocate(cap);
    }

    private HdfsResponse doOpen(HdfsRequest request)
            throws IOException {
        String requestPath = request.getPath();
        Path path = new Path(requestPath);

        FileSystem fs = FileSystem.get(URI.create(requestPath), configuration);
        fs.setWorkingDirectory(new Path("/"));

        CacheValue cv = new CacheValue();
        cv.fs = fs;
        cv.path = path;
        cv.inputStream = fs.open(path);
        cv.buffer = newBuffer(1024 * 1024);

        String sessionId = UUID.randomUUID().toString();
        cache.put(sessionId, cv);
        HdfsResponse resp =
                HdfsResponse.newBuilder().setSessionId(sessionId).build();
        return resp;
    }

    private HdfsResponse doClose(HdfsRequest request) {
        cache.invalidate(request.getSessionId());
        return HdfsResponse.getDefaultInstance();
    }

    private CacheValue getCacheValue(String sessionId) throws IOException {
        CacheValue cv = cache.getIfPresent(sessionId);
        if (cv == null) {
            throw new IOException(String.format("session id %s not found", sessionId));
        }
        return cv;
    }

    private HdfsResponse doRead(HdfsRequest request)
            throws IOException {
        int size = request.getSize();
        CacheValue cv = getCacheValue(request.getSessionId());

        cv.buffer = resizeBuffer(cv.buffer, request.getSize());
        cv.inputStream.readFully(request.getOffset(), cv.buffer.array(), 0, size);
        ByteString bs = ByteString.copyFrom(cv.buffer.array(), 0, size);
        //        ByteString bs = ByteString.copyFrom(cv.buffer, size);
        HdfsResponse resp =
                HdfsResponse.newBuilder().setData(bs).build();
        return resp;
    }

    private HdfsResponse doGetSize(HdfsRequest request)
            throws IOException {
        CacheValue cv = getCacheValue(request.getSessionId());
        FileStatus st = cv.fs.getFileStatus(cv.path);
        HdfsResponse resp =
                HdfsResponse.newBuilder().setSize(st.getLen()).build();
        return resp;
    }

    private HdfsResponse doGetStats(HdfsRequest request)
            throws IOException {
        CacheValue cv = getCacheValue(request.getSessionId());
        HdfsStats.Builder stats = HdfsStats.newBuilder();

        if (cv.inputStream instanceof HdfsDataInputStream) {
            HdfsDataInputStream inputStream = (HdfsDataInputStream) cv.inputStream;
            DFSInputStream.ReadStatistics st = inputStream.getReadStatistics();
            stats.setTotalBytesRead(st.getTotalBytesRead())
                    .setTotalLocalBytesRead(st.getTotalLocalBytesRead())
                    .setTotalShortCircuitBytesRead(st.getTotalShortCircuitBytesRead());
        }
        HdfsResponse resp =
                HdfsResponse.newBuilder().setStats(stats).build();
        return resp;
    }

    // ============================================================

    @Override
    public void hdfsOpen(HdfsRequest request,
                         StreamObserver<HdfsResponse> responseObserver) {
        try {
            logger.info(
                    String.format("[%d][SS] hdfsOpen", rpcOnFly.incrementAndGet()));
            responseObserver.onNext(doOpen(request));
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(e);
        } finally {
            logger.info(
                    String.format("[%d][EE] hdfsOpen", rpcOnFly.decrementAndGet()));
        }
    }

    @Override
    public void hdfsClose(HdfsRequest request,
                          StreamObserver<HdfsResponse> responseObserver) {
        logger.info(
                String.format("[%d][SS] hdfsClose", rpcOnFly.incrementAndGet()));
        responseObserver.onNext(doClose(request));
        responseObserver.onCompleted();
        logger.info(
                String.format("[%d][EE] hdfsClose", rpcOnFly.decrementAndGet()));
    }

    @Override
    public void hdfsRead(HdfsRequest request,
                         StreamObserver<HdfsResponse> responseObserver) {
        try {
            logger.info(
                    String.format("[%d][SS] hdfsRead", rpcOnFly.incrementAndGet()));
            responseObserver.onNext(doRead(request));
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(e);
        } finally {
            logger.info(
                    String.format("[%d][EE] hdfsRead", rpcOnFly.decrementAndGet()));
        }
    }

    @Override
    public void hdfsGetSize(HdfsRequest request,
                            StreamObserver<HdfsResponse> responseObserver) {
        try {
            responseObserver.onNext(doGetSize(request));
            responseObserver.onCompleted();

        } catch (IOException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void hdfsGetStats(HdfsRequest request,
                             StreamObserver<HdfsResponse> responseObserver) {
        try {
            responseObserver.onNext(doGetStats(request));
            responseObserver.onCompleted();

        } catch (IOException e) {
            responseObserver.onError(e);
        }
    }
}
