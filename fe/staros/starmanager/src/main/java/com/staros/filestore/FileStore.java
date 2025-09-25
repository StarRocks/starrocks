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

package com.staros.filestore;

import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;

import java.util.Map;

public interface FileStore {
    String key();

    String name();

    String getComment();

    void setEnabled(boolean enabled);

    boolean getEnabled();

    boolean isBuiltin();

    void setBuiltin(boolean builtin);

    long getVersion();

    void setVersion(long version);

    boolean isValid();

    void increaseVersion();

    FileStoreType type();

    FileStoreInfo toProtobuf();

    FileStoreInfo toDebugProtobuf();

    void mergeFrom(FileStore other);

    String rootPath();

    boolean isPartitionedPrefixEnabled();

    int numOfPartitionedPrefix();

    Map<String, String> getPropertiesMap();

    static FileStore fromProtobuf(FileStoreInfo fsInfo) {
        return AbstractFileStore.fromProtobuf(fsInfo);
    }
}
