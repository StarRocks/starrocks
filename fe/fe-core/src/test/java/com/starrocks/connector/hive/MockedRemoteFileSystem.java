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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MockedRemoteFileSystem extends FileSystem {
    public static final String HDFS_HOST = "hdfs://127.0.0.1:10000";
    public static final String HDFS_HIVE_TABLE = HDFS_HOST + "/hive.db/hive_tbl";
    public static final String HDFS_RECURSIVE_TABLE = HDFS_HOST + "/hive.db/recursive_tbl";

    private Map<String, List<LocatedFileStatus>> fileEntries;
    private String hdfsTable;

    public MockedRemoteFileSystem(String tbl) {
        if (tbl == HDFS_HIVE_TABLE) {
            this.fileEntries = createHiveEntries();
        } else if (tbl == HDFS_RECURSIVE_TABLE) {
            this.fileEntries = createRecursiveEntries();
        }
        this.hdfsTable = tbl;
    }

    private Map<String, List<LocatedFileStatus>> createHiveEntries() {
        Map<String, List<LocatedFileStatus>> hiveEntries = new HashMap<String, List<LocatedFileStatus>>();
        List<LocatedFileStatus> tblDirs = ImmutableList.of(
                locatedFileStatus(new Path(HDFS_HIVE_TABLE + "/000000_0"), false)
                );
        hiveEntries.put(HDFS_HIVE_TABLE, tblDirs);

        return hiveEntries;
    }

    private Map<String, List<LocatedFileStatus>> createRecursiveEntries() {
        Map<String, List<LocatedFileStatus>> recEntries = new HashMap<String, List<LocatedFileStatus>>();
        List<LocatedFileStatus> tblDirs = ImmutableList.of(
                locatedFileStatus(new Path(HDFS_RECURSIVE_TABLE + "/subdir1"), true),
                locatedFileStatus(new Path(HDFS_RECURSIVE_TABLE + "/subdir2"), true),
                locatedFileStatus(new Path(HDFS_RECURSIVE_TABLE + "/.subdir3"), true)
                );
        recEntries.put(HDFS_RECURSIVE_TABLE, tblDirs);

        List<LocatedFileStatus> subDir1 = ImmutableList.of(
                locatedFileStatus(new Path(HDFS_RECURSIVE_TABLE + "/subdir1/000000_0"), false),
                locatedFileStatus(new Path(HDFS_RECURSIVE_TABLE + "/subdir1/000000_1"), false)
                );
        recEntries.put(HDFS_RECURSIVE_TABLE + "/subdir1", subDir1);

        List<LocatedFileStatus> subDir2 = ImmutableList.of(
                locatedFileStatus(new Path(HDFS_RECURSIVE_TABLE + "/subdir2/.000000_2"), false)
                );
        recEntries.put(HDFS_RECURSIVE_TABLE + "/subdir2", subDir2);

        List<LocatedFileStatus> subDir3 = ImmutableList.of(
                locatedFileStatus(new Path(HDFS_RECURSIVE_TABLE + "/.subdir3/000000_3"), false)
                );
        recEntries.put(HDFS_RECURSIVE_TABLE + "/.subdir3", subDir3);

        return recEntries;
    }

    @Override
    public boolean exists(Path f) throws IOException {
        return false;
    }

    public static LocatedFileStatus locatedFileStatus(Path path, boolean isDir) {
        return locatedFileStatus(path, isDir, 20, 1234567890);
    }

    public static LocatedFileStatus locatedFileStatus(Path path, boolean isDir, long fileLength, long modificationTime) {
        return new LocatedFileStatus(
                fileLength,
                isDir,
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

    public static String formatFilePath(String filepath) {
        if (filepath.startsWith("/")) {
            filepath = HDFS_HOST + filepath;
        }
        filepath = filepath.replaceAll(" ", "");
        return filepath;
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) {
        return new RemoteIterator<LocatedFileStatus>() {
            private final Iterator<LocatedFileStatus> iterator = locatedFileList(f).iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public LocatedFileStatus next() {
                return iterator.next();
            }

            public List<LocatedFileStatus> locatedFileList(Path f) {
                String key = hdfsTable == HDFS_HIVE_TABLE ? HDFS_HIVE_TABLE : formatFilePath(f.toString());
                return fileEntries.get(key);
            }
        };
    }

    @Override
    public FileStatus[] globStatus(Path path) {
        FileStatus fileStatus = null;
        if (this.hdfsTable.equals(HDFS_HIVE_TABLE)) {
            fileStatus = new FileStatus(
                    0, false, 0, 0, 0, new Path(HDFS_HIVE_TABLE));
        } else if (this.hdfsTable.equals(HDFS_RECURSIVE_TABLE)) {
            fileStatus = new FileStatus(
                    0, false, 0, 0, 0, new Path(HDFS_RECURSIVE_TABLE));
        }

        return new FileStatus[] {fileStatus};
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
        return false;
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
        return false;
    }

    @Override
    public FileStatus getFileStatus(Path path) {
        if (hdfsTable == HDFS_HIVE_TABLE) {
            return fileEntries.get(HDFS_HIVE_TABLE).get(0);
        }

        Path parent = path.getParent();
        List<LocatedFileStatus> entries = fileEntries.get(formatFilePath(parent.toString()));
        for (int i = 0; i < entries.size(); ++i) {
            if (entries.get(i).getPath() == path) {
                return entries.get(i);
            }
        }
        return null;
    }
}
