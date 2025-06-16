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
import com.starrocks.connector.hive.TextFileFormatDesc;

public class RemoteFileDesc {
    protected final String fileName;
    // Optional.
    // The full path of the remote file.
    protected String fullPath;
    protected final String compression;
    protected final long length;
    protected final long modificationTime;
    protected final ImmutableList<RemoteFileBlockDesc> blockDescs;
    protected boolean splittable;
    protected TextFileFormatDesc textFileFormatDesc;

    public RemoteFileDesc(String fileName, String compression, long length, long modificationTime,
                          ImmutableList<RemoteFileBlockDesc> blockDescs) {
        this.fileName = fileName;
        this.compression = compression;
        this.length = length;
        this.modificationTime = modificationTime;
        this.blockDescs = blockDescs;
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

    public RemoteFileDesc setSplittable(boolean splittable) {
        this.splittable = splittable;
        return this;
    }

    public TextFileFormatDesc getTextFileFormatDesc() {
        return textFileFormatDesc;
    }

    public RemoteFileDesc setTextFileFormatDesc(TextFileFormatDesc textFileFormatDesc) {
        this.textFileFormatDesc = textFileFormatDesc;
        return this;
    }

    public String getFullPath() {
        return this.fullPath;
    }

    public RemoteFileDesc setFullPath(String fullPath) {
        this.fullPath = fullPath;
        return this;
    }

    @Override
    public String toString() {
        return "RemoteFileDesc{" + "fileName='" + fileName + '\'' +
                "fullPath='" + fullPath + '\'' +
                ", compression='" + compression + '\'' +
                ", length=" + length +
                ", modificationTime=" + modificationTime +
                ", blockDescs=" + blockDescs +
                ", splittable=" + splittable +
                ", textFileFormatDesc=" + textFileFormatDesc +
                '}';
    }
}

