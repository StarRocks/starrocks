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

package com.staros.filecache;

import com.staros.proto.FileCacheInfo;

public class FileCache {
    private boolean enableCache;
    private final boolean asyncWriteBack;

    public FileCache() {
        this.enableCache = true;
        this.asyncWriteBack = false;
    }

    public FileCache(boolean enableCache, boolean asyncWriteBack) {
        this.enableCache = enableCache;
        this.asyncWriteBack = asyncWriteBack;
    }

    public void setFileCacheEnable(boolean enableCache) {
        this.enableCache = enableCache;
    }

    public boolean getFileCacheEnable() {
        return this.enableCache;
    }

    public FileCacheInfo toProtobuf() {
        return FileCacheInfo.newBuilder()
                .setEnableCache(enableCache)
                .setAsyncWriteBack(asyncWriteBack)
                .build();
    }

    public static FileCache fromProtobuf(FileCacheInfo cacheInfo) {
        return new FileCache(cacheInfo.getEnableCache(), cacheInfo.getAsyncWriteBack());
    }
}
