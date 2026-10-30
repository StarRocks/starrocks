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

// This file is based on code available under the Apache license here:
// https://github.com/apache/hadoop/blob/rel/release-3.4.3/hadoop-tools/hadoop-aliyun/src/main/java/org/apache/hadoop/fs/aliyun/oss/ReadBuffer.java

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is used by {@link AliyunOSSInputStream}
 * and {@link AliyunOSSFileReaderTask} to buffer data that read from oss.
 *
 * StarRocks: {@link #await} is bounded. AliyunOSSInputStream.reopen() waits here for the read-ahead task to fill the
 * buffer, but that task can be lost without signalling: a shut-down executor only logs the rejected task, and an
 * Error thrown inside the task skips signalAll(). The reader then parked forever with no timeout. After the bound,
 * the buffer is marked ERROR, so reopen() fails with an IOException instead.
 */
public class ReadBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(ReadBuffer.class);

    // Far above a 512 KB part read that succeeds or fails through the OSS client's own timeouts and retries.
    static long awaitTimeoutNanos = TimeUnit.MINUTES.toNanos(10);

    enum STATUS {
        INIT, SUCCESS, ERROR
    }

    private final ReentrantLock lock = new ReentrantLock();

    private Condition readyCondition = lock.newCondition();

    private byte[] buffer;
    private STATUS status;
    private long byteStart;
    private long byteEnd;

    public ReadBuffer(long byteStart, long byteEnd) {
        this.buffer = new byte[(int) (byteEnd - byteStart) + 1];

        this.status = STATUS.INIT;
        this.byteStart = byteStart;
        this.byteEnd = byteEnd;
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void await(STATUS waitStatus) throws InterruptedException {
        long remainingNanos = awaitTimeoutNanos;
        while (this.status == waitStatus) {
            if (remainingNanos <= 0) {
                LOG.warn("Read-ahead of bytes [{}, {}] did not finish within {} s, failing the read",
                        byteStart, byteEnd, TimeUnit.NANOSECONDS.toSeconds(awaitTimeoutNanos));
                this.status = STATUS.ERROR;
                return;
            }
            remainingNanos = readyCondition.awaitNanos(remainingNanos);
        }
    }

    public void signalAll() {
        readyCondition.signalAll();
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public STATUS getStatus() {
        return status;
    }

    public void setStatus(STATUS status) {
        this.status = status;
    }

    public long getByteStart() {
        return byteStart;
    }

    public long getByteEnd() {
        return byteEnd;
    }
}
