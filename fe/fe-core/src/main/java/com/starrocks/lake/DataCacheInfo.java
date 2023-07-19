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
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;

import java.io.IOException;

public class DataCacheInfo implements GsonPreProcessable, GsonPostProcessable {
    @SerializedName(value = "cacheInfoBytes")
    private byte[] cacheInfoBytes;
    private FileCacheInfo cacheInfo;

    public DataCacheInfo(FileCacheInfo cacheInfo) {
        this.cacheInfo = cacheInfo;
    }

    public DataCacheInfo(boolean enableCache, boolean asyncWriteBack) {
        long ttl = enableCache ? -1 /* -1 means no TTL */ : 0;
        this.cacheInfo = FileCacheInfo.newBuilder().setEnableCache(enableCache).setTtlSeconds(ttl)
                .setAsyncWriteBack(asyncWriteBack).build();
    }

    public FileCacheInfo getCacheInfo() {
        return cacheInfo;
    }

    public boolean isEnabled() {
        return cacheInfo.getEnableCache();
    }

    public boolean isAsyncWriteBack() {
        return cacheInfo.getAsyncWriteBack();
    }

    @Override
    public void gsonPreProcess() throws IOException {
        if (cacheInfo != null) {
            cacheInfoBytes = cacheInfo.toByteArray();
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (cacheInfoBytes != null) {
            cacheInfo = FileCacheInfo.parseFrom(cacheInfoBytes);
        }
    }
}
