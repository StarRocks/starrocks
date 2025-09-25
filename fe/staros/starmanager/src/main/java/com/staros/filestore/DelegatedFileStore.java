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

import com.google.common.base.Preconditions;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;

import java.util.Map;

public class DelegatedFileStore implements FileStore {
    private FileStore innerFileStore;

    public DelegatedFileStore(FileStore innerFs) {
        Preconditions.checkNotNull(innerFs);
        this.innerFileStore = innerFs;
    }

    @Override
    public String key() {
        return innerFileStore.key();
    }

    @Override
    public String name() {
        return innerFileStore.name();
    }

    @Override
    public String getComment() {
        return innerFileStore.getComment();
    }

    @Override
    public void setEnabled(boolean enabled) {
        innerFileStore.setEnabled(enabled);
    }

    @Override
    public boolean getEnabled() {
        return innerFileStore.getEnabled();
    }

    @Override
    public boolean isBuiltin() {
        return innerFileStore.isBuiltin();
    }

    @Override
    public void setBuiltin(boolean builtin) {
        innerFileStore.setBuiltin(builtin);
    }

    @Override
    public long getVersion() {
        return innerFileStore.getVersion();
    }

    @Override
    public void setVersion(long version) {
        innerFileStore.setVersion(version);
    }

    @Override
    public boolean isValid() {
        return innerFileStore.isValid();
    }

    @Override
    public void increaseVersion() {
        innerFileStore.increaseVersion();
    }

    @Override
    public FileStoreType type() {
        return innerFileStore.type();
    }

    @Override
    public FileStoreInfo toProtobuf() {
        return innerFileStore.toProtobuf();
    }

    @Override
    public FileStoreInfo toDebugProtobuf() {
        return innerFileStore.toDebugProtobuf();
    }

    @Override
    public void mergeFrom(FileStore other) {
        innerFileStore.mergeFrom(other);
    }

    @Override
    public String rootPath() {
        return innerFileStore.rootPath();
    }

    @Override
    public boolean isPartitionedPrefixEnabled() {
        return innerFileStore.isPartitionedPrefixEnabled();
    }

    @Override
    public int numOfPartitionedPrefix() {
        return innerFileStore.numOfPartitionedPrefix();
    }

    @Override
    public Map<String, String> getPropertiesMap() {
        return innerFileStore.getPropertiesMap();
    }

    public void swapDelegation(FileStoreInfo fsInfo) {
        FileStore fs = FileStore.fromProtobuf(fsInfo);
        Preconditions.checkNotNull(fs);
        innerFileStore = fs;
    }
}
