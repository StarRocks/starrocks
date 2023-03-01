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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.security.SecureRandom;

public class IOUtil {
    public static final String FILE_PREFIX = "file://";
    public static final String FILE_SIMPLIFIED_PREFIX = "file:/";
    public static final String S3A_PREFIX = "s3a://";
    public static final String EMPTY_PREFIX = "";
    public static SecureRandom rand = new SecureRandom();

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

    public static OutputFile getTmpOutputFile(String localDir, String path) {
        String newPath = s3aToLocalTmpFilePath(localDir, path);
        return HadoopOutputFile.fromLocation(newPath, new Configuration());
    }

    public static OutputFile getOutputFile(String localDir, String path) {
        Path newPath = s3aToLocalFilePath(localDir, path);
        return HadoopOutputFile.fromPath(newPath, new Configuration());
    }

    public static OutputFile getOutputFile(Path path) {
        return HadoopOutputFile.fromPath(path, new Configuration());
    }

    public static Path localFileToS3a(Path localFile, String localDir) {
        String s3aPath = localFile.toString().replace(FILE_PREFIX, S3A_PREFIX)
                .replace(FILE_SIMPLIFIED_PREFIX, S3A_PREFIX)
                .replace(localDir, EMPTY_PREFIX);
        return new Path(s3aPath);
    }

    public static String s3aToLocalTmpFilePath(String localDir, String path) {
        String newPath = path.replace(S3A_PREFIX, EMPTY_PREFIX);
        return Paths.get(FILE_PREFIX + localDir, newPath + ".tmp" + rand.nextInt(100)).toString();
    }

    public static Path s3aToLocalFilePath(String localDir, String path) {
        String newPath = path.replace(S3A_PREFIX, EMPTY_PREFIX);
        return new Path(FILE_PREFIX + localDir, newPath);
    }

    public static Path getLocalDiskDirPath(String localDir) {
        return new Path(FILE_PREFIX + localDir);
    }
}