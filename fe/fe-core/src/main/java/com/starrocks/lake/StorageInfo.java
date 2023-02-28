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


package com.starrocks.lake;

import com.google.gson.annotations.SerializedName;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;

import java.io.IOException;

// Storage info for lake table, include object storage info and table default cache info.
// Currently, storage group is table level.
public class StorageInfo implements GsonPreProcessable, GsonPostProcessable {
    @SerializedName(value = "storeInfoBytes")
    private byte[] storeInfoBytes;
    private FilePathInfo storeInfo;

    @SerializedName(value = "cacheInfoBytes")
    private byte[] cacheInfoBytes;
    private FileCacheInfo cacheInfo;

    public StorageInfo(FilePathInfo storeInfo, FileCacheInfo cacheInfo) {
        this.storeInfo = storeInfo;
        this.cacheInfo = cacheInfo;
    }

    public boolean isEnableStorageCache() {
        return getCacheInfo().getEnableCache();
    }

    public long getStorageCacheTtlS() {
        return getCacheInfo().getTtlSeconds();
    }

    public boolean isEnableAsyncWriteBack() {
        return getCacheInfo().getAsyncWriteBack();
    }

    public FilePathInfo getFilePathInfo() {
        return storeInfo;
    }

    public FileCacheInfo getCacheInfo() {
        return cacheInfo;
    }

    public StorageCacheInfo getStorageCacheInfo() {
        return new StorageCacheInfo(getCacheInfo());
    }

    @Override
    public void gsonPreProcess() throws IOException {
        if (storeInfo != null) {
            storeInfoBytes = storeInfo.toByteArray();
        }
        if (cacheInfo != null) {
            cacheInfoBytes = cacheInfo.toByteArray();
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (storeInfoBytes != null) {
            storeInfo = FilePathInfo.parseFrom(storeInfoBytes);
        }
        if (cacheInfoBytes != null) {
            cacheInfo = FileCacheInfo.parseFrom(cacheInfoBytes);
        }
    }
}
