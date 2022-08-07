package com.starrocks.external.hive;

import com.google.common.collect.ImmutableList;
import com.starrocks.external.RemoteFileBlockDesc;
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
                        ImmutableList<RemoteFileBlockDesc> blockDescs, boolean splittable,
                        TextFileFormatDesc textFileFormatDesc) {
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
}
