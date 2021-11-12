// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableList;

public class HdfsFileDesc {
    private String fileName;
    private String compression;
    private long length;
    private ImmutableList<HdfsFileBlockDesc> blockDescs;
    private boolean splittable;

    public HdfsFileDesc(String fileName, String compression, long length,
                        ImmutableList<HdfsFileBlockDesc> blockDescs) {
        this.fileName = fileName;
        this.compression = compression;
        this.length = length;
        this.blockDescs = blockDescs;
        this.splittable = false;
    }

    public HdfsFileDesc(String fileName, String compression, long length,
                        ImmutableList<HdfsFileBlockDesc> blockDescs, boolean splittable) {
        this(fileName, compression, length, blockDescs);
        this.splittable = splittable;
    }

    public String getFileName() {
        return fileName;
    }

    public String getCompression() {
        return compression;
    }

    public long getLength() {
        return length;
    }

    public ImmutableList<HdfsFileBlockDesc> getBlockDescs() {
        return blockDescs;
    }

    public boolean isSplittable() {
        return splittable;
    }
}
