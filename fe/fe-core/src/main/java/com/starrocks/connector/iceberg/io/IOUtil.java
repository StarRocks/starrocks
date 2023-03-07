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
    public static final String EMPTY_STRING = "";
    public static final String SLASH_STRING = "/";

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

    public static Path getLocalDiskDirPath(String localDir) {
        return new Path(FILE_PREFIX + localDir);
    }

    public static OutputFile getTmpOutputFile(String localDir, String path) {
        String newPath = remoteToLocalTmpFilePath(localDir, path);
        return HadoopOutputFile.fromLocation(newPath, new Configuration());
    }

    public static OutputFile getOutputFile(Path path) {
        return HadoopOutputFile.fromPath(path, new Configuration());
    }

    public static OutputFile getOutputFile(String localDir, String path) {
        Path newPath = new Path(remoteToLocalFilePath(localDir, path));
        return HadoopOutputFile.fromPath(newPath, new Configuration());
    }

    public static String localFileToRemote(Path localFile, String localDir) {
        String localPath = localFile.toString().replace(FILE_PREFIX, SLASH_STRING)
                .replace(FILE_SIMPLIFIED_PREFIX, SLASH_STRING)
                .replace(localDir, EMPTY_STRING);
        int split  = 0;
        // we need identify weather localDir is ended with '/'
        // and then we need fetch the schema and transform to remote path string
        if (localPath.startsWith("/")) {
            split = localPath.indexOf("/", 1);
            return localPath.substring(1, split) + "://" + localPath.substring(split + 1);
        } else {
            split = localPath.indexOf("/");
            return localPath.substring(0, split) + "://" + localPath.substring(split + 1);
        }
    }

    public static String remoteToLocalTmpFilePath(String localDir, String path) {
        Path remotePath = new Path(path);
        String prefix = remotePath.toUri().getScheme();
        String newPath = path.substring(path.indexOf("/"));
        return Paths.get(FILE_PREFIX + localDir, prefix, newPath + ".tmp" + rand.nextInt(100)).toString();
    }

    public static String remoteToLocalFilePath(String localDir, String path) {
        Path remotePath = new Path(path);
        String prefix = remotePath.toUri().getScheme();
        String newPath = path.substring(path.indexOf("/"));
        return Paths.get(FILE_PREFIX + localDir, prefix, newPath).toString();
    }
}