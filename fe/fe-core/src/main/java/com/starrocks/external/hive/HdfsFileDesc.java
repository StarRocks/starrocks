// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;
import com.starrocks.external.hive.text.TextFileFormatDesc;

public class HdfsFileDesc {
    @SerializedName(value = "fileName")
    private String fileName;
    @SerializedName(value = "compression")
    private String compression;
    // length == -1 and fileName == "" when the table type is hudi MOR table
    // and the base file is not exists (log files only).
    @SerializedName(value = "length")
    private long length;
    private ImmutableList<HdfsFileBlockDesc> blockDescs;
    @SerializedName(value = "splittable")
    private boolean splittable;
    private TextFileFormatDesc textFileFormatDesc;
    private ImmutableList<String> hudiDeltaLogs;

    public HdfsFileDesc(String fileName, String compression, long length,
                        ImmutableList<HdfsFileBlockDesc> blockDescs, ImmutableList<String> hudiDeltaLogs,
                        boolean splittable, TextFileFormatDesc textFileFormatDesc) {
        this.fileName = fileName;
        this.compression = compression;
        this.length = length;
        this.blockDescs = blockDescs;
        this.hudiDeltaLogs = hudiDeltaLogs;
        this.splittable = splittable;
        this.textFileFormatDesc = textFileFormatDesc;
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

    public TextFileFormatDesc getTextFileFormatDesc() {
        return textFileFormatDesc;
    }

    public ImmutableList<String> getHudiDeltaLogs() {
        return hudiDeltaLogs;
    }

}
