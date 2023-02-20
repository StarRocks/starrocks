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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/io/IOUtils.java

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

package com.starrocks.common.io;

import com.google.common.base.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

/**
 * An utility class for I/O related functionality.
 */
public class IOUtils {

    /**
     * Copies from one stream to another.
     *
     * @param in       InputStrem to read from
     * @param out      OutputStream to write to
     * @param buffSize the size of the buffer
     * @param speed    limit of the copy (B/s)
     * @param close    whether or not close the InputStream and OutputStream at the
     *                 end. The streams are closed in the finally clause.
     */
    public static long copyBytes(InputStream in, OutputStream out,
                                 int buffSize, int speed, boolean close) throws IOException {

        PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
        byte[] buf = new byte[buffSize];
        long bytesReadTotal = 0;
        long startTime = 0;
        long sleepTime = 0;
        long curTime = 0;
        try {
            if (speed > 0) {
                startTime = System.currentTimeMillis();
            }
            int bytesRead = in.read(buf);
            while (bytesRead >= 0) {
                out.write(buf, 0, bytesRead);
                if ((ps != null) && ps.checkError()) {
                    throw new IOException("Unable to write to output stream.");
                }
                bytesReadTotal += bytesRead;
                if (speed > 0) {
                    curTime = System.currentTimeMillis();
                    sleepTime = bytesReadTotal / speed * 1000 / 1024
                            - (curTime - startTime);
                    if (sleepTime > 0) {
                        try {
                            Thread.sleep(sleepTime);
                        } catch (InterruptedException ie) {
                        }
                    }
                }

                bytesRead = in.read(buf);
            }
            return bytesReadTotal;
        } finally {
            if (close) {
                out.close();
                in.close();
            }
        }
    }

    public static void writeOptionString(DataOutput output, String value) throws IOException {
        boolean hasValue = !Strings.isNullOrEmpty(value);
        output.writeBoolean(hasValue);
        if (hasValue) {
            Text.writeString(output, value);
        }
    }

    public static String readOptionStringOrNull(DataInput input) throws IOException {
        if (input.readBoolean()) {
            return Text.readString(input);
        }
        return null;
    }
}
