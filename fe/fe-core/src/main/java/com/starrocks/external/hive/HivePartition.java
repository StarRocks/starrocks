// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.DescriptorTable;

public class HivePartition {
    private HdfsFileFormat format;
    private ImmutableList<HdfsFileDesc> files;
    private String fullPath;
    private DescriptorTable.ReferencedPartitionInfo partitionInfo;

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

    public DescriptorTable.ReferencedPartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(DescriptorTable.ReferencedPartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

}
