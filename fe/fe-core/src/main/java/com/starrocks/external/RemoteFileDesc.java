// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external;

import com.google.common.collect.ImmutableList;
import com.starrocks.external.hive.text.TextFileFormatDesc;

public class RemoteFileDesc {
    private String fileName;
    private String compression;
    private long length;
    private ImmutableList<RemoteFileBlockDesc> blockDescs;
    private boolean splittable;
    private TextFileFormatDesc textFileFormatDesc;

    public RemoteFileDesc(String fileName, String compression, long length,
                          ImmutableList<RemoteFileBlockDesc> blockDescs) {
        this.fileName = fileName;
        this.compression = compression;
        this.length = length;
        this.blockDescs = blockDescs;
        this.splittable = false;
    }

    public RemoteFileDesc(String fileName, String compression, long length,
                          ImmutableList<RemoteFileBlockDesc> blockDescs,
                          boolean splittable, TextFileFormatDesc textFileFormatDesc) {
        this(fileName, compression, length, blockDescs);
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

    public ImmutableList<RemoteFileBlockDesc> getBlockDescs() {
        return blockDescs;
    }

    public boolean isSplittable() {
        return splittable;
    }

    public TextFileFormatDesc getTextFileFormatDesc() {
        return textFileFormatDesc;
    }

    public RemoteFileDesc setSplittable(boolean splittable) {
        this.splittable = splittable;
        return this;
    }

    public RemoteFileDesc setTextFileFormatDesc(TextFileFormatDesc textFileFormatDesc) {
        this.textFileFormatDesc = textFileFormatDesc;
        return this;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("RemoteFileDesc{");
        sb.append("fileName='").append(fileName).append('\'');
        sb.append(", compression='").append(compression).append('\'');
        sb.append(", length=").append(length);
        sb.append(", blockDescs=").append(blockDescs);
        sb.append(", splittable=").append(splittable);
        sb.append(", textFileFormatDesc=").append(textFileFormatDesc);
        sb.append('}');
        return sb.toString();
    }
}
