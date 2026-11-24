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
import com.google.protobuf.ByteString;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.persist.proto.StorageInfoPB;

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

    public boolean isEnableDataCache() {
        return getCacheInfo().getEnableCache();
    }

    public boolean isEnableAsyncWriteBack() {
        return getCacheInfo().getAsyncWriteBack();
    }

    public FilePathInfo getFilePathInfo() {
        return storeInfo;
    }

    public void setFilePathInfo(FilePathInfo filePathInfo) {
        this.storeInfo = filePathInfo;
    }

    public FileCacheInfo getCacheInfo() {
        return cacheInfo;
    }

    public DataCacheInfo getDataCacheInfo() {
        return new DataCacheInfo(getCacheInfo());
    }

    public void setDataCacheEnable(boolean isEnable) {
        this.cacheInfo = this.cacheInfo.toBuilder().setEnableCache(isEnable).build();
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

    public StorageInfoPB toProto() {
        StorageInfoPB.Builder builder = StorageInfoPB.newBuilder();
        // Same rule as gsonPreProcess: the live messages win over the bytes they were loaded from.
        byte[] storeBytes = storeInfo != null ? storeInfo.toByteArray() : storeInfoBytes;
        if (storeBytes != null) {
            builder.setStoreInfo(ByteString.copyFrom(storeBytes));
        }
        byte[] cacheBytes = cacheInfo != null ? cacheInfo.toByteArray() : cacheInfoBytes;
        if (cacheBytes != null) {
            builder.setCacheInfo(ByteString.copyFrom(cacheBytes));
        }
        return builder.build();
    }

    public static StorageInfo fromProto(StorageInfoPB pb) throws IOException {
        byte[] storeBytes = pb.hasStoreInfo() ? pb.getStoreInfo().toByteArray() : null;
        byte[] cacheBytes = pb.hasCacheInfo() ? pb.getCacheInfo().toByteArray() : null;
        StorageInfo info = new StorageInfo(storeBytes != null ? FilePathInfo.parseFrom(storeBytes) : null,
                cacheBytes != null ? FileCacheInfo.parseFrom(cacheBytes) : null);
        info.storeInfoBytes = storeBytes;
        info.cacheInfoBytes = cacheBytes;
        return info;
    }
}
