// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/iceberg/blob/master/core/src/main/java/org/apache/iceberg/io/IOUtil.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.iceberg.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class IOUtil {
    // not meant to be instantiated
    private IOUtil() {
    }

    public static void readFully(InputStream stream, byte[] bytes, int offset, int length) throws IOException {
        int bytesRead = readRemaining(stream, bytes, offset, length);
        if (bytesRead < length) {
            throw new EOFException(
                "Reached the end of stream with " + (length - bytesRead) + " bytes left to read");
        }
    }


    public static int readRemaining(InputStream stream, byte[] bytes, int offset, int length) throws IOException {
        int pos = offset;
        int remaining = length;
        while (remaining > 0) {
            int bytesRead = stream.read(bytes, pos, remaining);
            if (bytesRead < 0) {
                break;
            }

            remaining -= bytesRead;
            pos += bytesRead;
        }

        return length - remaining;
    }
}