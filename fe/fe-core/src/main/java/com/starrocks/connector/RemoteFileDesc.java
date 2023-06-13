// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.google.common.collect.ImmutableList;
import com.starrocks.connector.hive.TextFileFormatDesc;
import com.starrocks.connector.paimon.PaimonSplitsInfo;

public class RemoteFileDesc {
    private String fileName;
    private String compression;
    private long length;
    private ImmutableList<RemoteFileBlockDesc> blockDescs;
    private boolean splittable;
    private TextFileFormatDesc textFileFormatDesc;
    private ImmutableList<String> hudiDeltaLogs;
    private PaimonSplitsInfo paimonSplitsInfo;

    public RemoteFileDesc(String fileName, String compression, long length,
                          ImmutableList<RemoteFileBlockDesc> blockDescs, ImmutableList<String> hudiDeltaLogs,
                          PaimonSplitsInfo paimonSplitsInfo) {
        this.fileName = fileName;
        this.compression = compression;
        this.length = length;
        this.blockDescs = blockDescs;
        this.hudiDeltaLogs = hudiDeltaLogs;
        this.paimonSplitsInfo = paimonSplitsInfo;
    }

    public static RemoteFileDesc createPamonRemoteFileDesc(PaimonSplitsInfo paimonSplitsInfo) {
        return new RemoteFileDesc(null, null, 0, null, null, paimonSplitsInfo);
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

    public ImmutableList<String> getHudiDeltaLogs() {
        return hudiDeltaLogs;
    }

    public PaimonSplitsInfo getPaimonSplitsInfo() {
        return paimonSplitsInfo;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RemoteFileDesc{");
        sb.append("fileName='").append(fileName).append('\'');
        sb.append(", compression='").append(compression).append('\'');
        sb.append(", length=").append(length);
        sb.append(", blockDescs=").append(blockDescs);
        sb.append(", splittable=").append(splittable);
        sb.append(", textFileFormatDesc=").append(textFileFormatDesc);
        sb.append(", hudiDeltaLogs=").append(hudiDeltaLogs);
        sb.append(", paimonSplitsInfo=").append(paimonSplitsInfo);
        sb.append('}');
        return sb.toString();
    }
}
