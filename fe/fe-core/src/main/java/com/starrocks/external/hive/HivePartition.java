// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableList;

public class HivePartition {
    private HdfsFileFormat format;
    private ImmutableList<HdfsFileDesc> files;
    private String fullPath;

    public HivePartition(HdfsFileFormat format, ImmutableList<HdfsFileDesc> files, String fullPath) {
        this.format = format;
        this.files = files;
        this.fullPath = fullPath;
    }

    public HdfsFileFormat getFormat() {
        return format;
    }

    public ImmutableList<HdfsFileDesc> getFiles() {
        return files;
    }

    public void setFiles(ImmutableList<HdfsFileDesc> files) {
        this.files = files;
    }

    public String getFullPath() {
        return fullPath;
    }

}
