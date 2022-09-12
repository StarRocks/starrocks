// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;

public class HdfsFileDesc {
    @SerializedName(value = "fileName")
    private String fileName;
    @SerializedName(value = "length")
    private long length;
    @SerializedName(value = "fileFormat")
    private HdfsFileFormat fileFormat;
    @SerializedName(value = "hiveTextFileDesc")
    private TextFileFormatDesc hiveTextFileDesc;

    private ImmutableList<HdfsFileBlockDesc> blockDescs;
    private ImmutableList<String> hudiDeltaLogs;
    public static final String INVALID_FILE_NAME = "";
    public static final long INVALID_FILE_LENGTH = -1L;

    public HdfsFileDesc(String fileName, long length,
                        HdfsFileFormat fileFormat,
                        TextFileFormatDesc hiveTextFileDesc,
                        ImmutableList<HdfsFileBlockDesc> blockDescs,
                        ImmutableList<String> hudiDeltaLogs) {
        this.fileName = fileName;
        this.length = length;
        this.fileFormat = fileFormat;
        this.hiveTextFileDesc = hiveTextFileDesc;
        this.blockDescs = blockDescs;
        this.hudiDeltaLogs = hudiDeltaLogs;
    }

    public String getFileName() {
        return fileName;
    }

    public long getLength() {
        return length;
    }

    public HdfsFileFormat getFileFormat() {
        return fileFormat;
    }

    public ImmutableList<HdfsFileBlockDesc> getBlockDescs() {
        return blockDescs;
    }

    public TextFileFormatDesc getTextFileFormatDesc() {
        return hiveTextFileDesc;
    }

    public ImmutableList<String> getHudiDeltaLogs() {
        return hudiDeltaLogs;
    }

    public boolean isSplittable() {
        return fileFormat.isSplittable(fileName);
    }

    public boolean isInvalidFileName() {
        return INVALID_FILE_NAME.equals(fileName);
    }

    public boolean isInvalidFileLength() {
        return length == INVALID_FILE_LENGTH;
    }

}
