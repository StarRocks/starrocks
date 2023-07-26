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


package com.starrocks.connector.hive;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

public class MockedRemoteFileSystem extends FileSystem {
    private final List<LocatedFileStatus> files;

    public static final String TEST_PATH_1_STR = "hdfs://127.0.0.1:10000/hive.db/hive_tbl/000000_0";
    public static final Path TEST_PATH_1 = new Path(TEST_PATH_1_STR);
    public static final List<LocatedFileStatus> TEST_FILES = ImmutableList.of(locatedFileStatus(TEST_PATH_1));

    public MockedRemoteFileSystem(List<LocatedFileStatus> files) {
        this.files = files;
    }

    public static LocatedFileStatus locatedFileStatus(Path path) {
        return locatedFileStatus(path, 20, 1234567890);
    }

    public static LocatedFileStatus locatedFileStatus(Path path, long fileLength, long modificationTime) {
        return new LocatedFileStatus(
                fileLength,
                false,
                0,
                0L,
                modificationTime,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {new BlockLocation(new String[] {"host1", "host2"},
                        new String[] {"localhost"}, 0, fileLength)});
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) {
        return new RemoteIterator<LocatedFileStatus>() {
            private final Iterator<LocatedFileStatus> iterator = files.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public LocatedFileStatus next() {
                return iterator.next();
            }
        };
    }

    @Override
    public URI getUri() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream create(Path path,
                                     FsPermission fsPermission,
                                     boolean b,
                                     int i,
                                     short i1,
                                     long l,
                                     Progressable progressable) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus[] listStatus(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setWorkingDirectory(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path getWorkingDirectory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus getFileStatus(Path path) {
        throw new UnsupportedOperationException();
    }
}
