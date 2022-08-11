// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.mysql.ssl;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface SSLChannel {
    boolean init() throws IOException;

    int readAll(ByteBuffer buffer) throws IOException;

    void write(ByteBuffer buffer) throws IOException;
}
