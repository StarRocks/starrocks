// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;

public class HivePartition {
    @SerializedName(value = "format")
    private RemoteFileInputFormat format;
    @SerializedName(value = "files")
    private ImmutableList<HdfsFileDesc> files;
    @SerializedName(value = "fullPath")
    private String fullPath;

    public HivePartition(RemoteFileInputFormat format, ImmutableList<HdfsFileDesc> files, String fullPath) {
        this.format = format;
        this.files = files;
        this.fullPath = fullPath;
    }

    public RemoteFileInputFormat getFormat() {
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
