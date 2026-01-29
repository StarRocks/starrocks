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

package com.staros.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Utils {
    private static final Logger LOG = LogManager.getLogger(Utils.class);

    public static void shutdownExecutorService(ExecutorService pool) {
        pool.shutdown();
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOG.error("Pool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    public static DigestInputStream getDigestInputStream(InputStream stream, String digestAlgorithm) {
        DigestInputStream digestInput = new DigestInputStream(stream, null);
        digestInput.on(false);
        try {
            MessageDigest md = MessageDigest.getInstance(digestAlgorithm);
            digestInput.setMessageDigest(md);
            digestInput.on(true);
        } catch (NoSuchAlgorithmException exception) {
            LOG.warn("Fail to get MessageDigest for algorithm:{}, disable checksum for InputStream!", digestAlgorithm);
        }
        return digestInput;
    }

    public static DigestOutputStream getDigestOutputStream(OutputStream stream, String digestAlgorithm) {
        DigestOutputStream digestOutput = new DigestOutputStream(stream, null);
        digestOutput.on(false);
        try {
            MessageDigest md = MessageDigest.getInstance(digestAlgorithm);
            digestOutput.setMessageDigest(md);
            digestOutput.on(true);
        } catch (NoSuchAlgorithmException exception) {
            LOG.warn("Fail to get MessageDigest for algorithm:{}, disable checksum for OutputStream!", digestAlgorithm);
        }
        return digestOutput;
    }

    public static void validateChecksum(MessageDigest digest, ByteString expectedChecksum) throws IOException {
        if (digest == null || expectedChecksum.isEmpty()) {
            return;
        }
        if (!ByteString.copyFrom(digest.digest()).equals(expectedChecksum)) {
            throw new IOException("checksum mismatch");
        }
    }

    public static ThreadFactory namedThreadFactory(String poolName) {
        return new ThreadFactoryBuilder().setNameFormat(poolName + "-%d").build();
    }

    public static void adjustFixedThreadPoolExecutors(ThreadPoolExecutor executor, int nExpectedThreads) {
        int nCurrent = executor.getCorePoolSize();
        if (nExpectedThreads < nCurrent) { // scale-in
            executor.setCorePoolSize(nExpectedThreads);
            executor.setMaximumPoolSize(nExpectedThreads);
        } else if (nExpectedThreads > nCurrent) { // scale-out
            executor.setMaximumPoolSize(nExpectedThreads);
            executor.setCorePoolSize(nExpectedThreads);
        }
    }

    public static void executeNoExceptionOrDie(Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable throwable) {
            LogUtils.fatal(LOG, "Don't expect any exception throw from the execution!", throwable);
            System.exit(-1);
        }
    }
}
