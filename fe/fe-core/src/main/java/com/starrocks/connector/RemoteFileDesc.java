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


package com.starrocks.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.connector.hive.TextFileFormatDesc;
import org.apache.iceberg.FileScanTask;

import java.util.ArrayList;
import java.util.List;

public class RemoteFileDesc {
    private String fileName;
    private String compression;
    private long length;
    private long modificationTime;
    private ImmutableList<RemoteFileBlockDesc> blockDescs;
    private boolean splittable;
    private TextFileFormatDesc textFileFormatDesc;
    private ImmutableList<String> hudiDeltaLogs;

    // Only this single RemoteFileDesc instance is used to record all iceberg scanTask
    // to reduce the memory usage of RemoteFileInfo
    private List<FileScanTask> icebergScanTasks = new ArrayList<>();
<<<<<<< HEAD
=======
    private PaimonSplitsInfo paimonSplitsInfo;

    private RemoteFileDesc(String fileName, String compression, long length, long modificationTime,
                          ImmutableList<RemoteFileBlockDesc> blockDescs, ImmutableList<String> hudiDeltaLogs,
                          List<FileScanTask> icebergScanTasks, PaimonSplitsInfo paimonSplitsInfo) {
        this.fileName = fileName;
        this.compression = compression;
        this.length = length;
        this.modificationTime = modificationTime;
        this.blockDescs = blockDescs;
        this.hudiDeltaLogs = hudiDeltaLogs;
        this.icebergScanTasks = icebergScanTasks;
        this.paimonSplitsInfo = paimonSplitsInfo;
    }
>>>>>>> c45aff1aa5 ([Enhancement] Encode file modification time to data cache key. (#27755))

    public RemoteFileDesc(String fileName, String compression, long length, long modificationTime,
                          ImmutableList<RemoteFileBlockDesc> blockDescs, ImmutableList<String> hudiDeltaLogs) {
        this.fileName = fileName;
        this.compression = compression;
        this.length = length;
        this.modificationTime = modificationTime;
        this.blockDescs = blockDescs;
        this.hudiDeltaLogs = hudiDeltaLogs;
    }

<<<<<<< HEAD
    public RemoteFileDesc(List<FileScanTask> tasks) {
        icebergScanTasks = Lists.newArrayList(tasks);
=======
    public static RemoteFileDesc createIcebergRemoteFileDesc(List<FileScanTask> tasks) {
        return new RemoteFileDesc(null, null, 0, 0, null, null, tasks, null);
    }

    public static RemoteFileDesc createPamonRemoteFileDesc(PaimonSplitsInfo paimonSplitsInfo) {
        return new RemoteFileDesc(null, null, 0, 0, null, null, null, paimonSplitsInfo);
>>>>>>> c45aff1aa5 ([Enhancement] Encode file modification time to data cache key. (#27755))
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

    public long getModificationTime() {
        return modificationTime;
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

    public List<FileScanTask> getIcebergScanTasks() {
        return icebergScanTasks;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RemoteFileDesc{");
        sb.append("fileName='").append(fileName).append('\'');
        sb.append(", compression='").append(compression).append('\'');
        sb.append(", length=").append(length);
        sb.append(", modificationTime=").append(modificationTime);
        sb.append(", blockDescs=").append(blockDescs);
        sb.append(", splittable=").append(splittable);
        sb.append(", textFileFormatDesc=").append(textFileFormatDesc);
        sb.append(", hudiDeltaLogs=").append(hudiDeltaLogs);
        sb.append('}');
        return sb.toString();
    }
}
