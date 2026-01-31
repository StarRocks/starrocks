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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractFileStore implements FileStore {
    protected String key;

    protected String name;

    protected List<String> locations = new ArrayList<>();

    protected boolean enabled = true;

    protected String comment = "";

    private boolean builtin = false;

    protected long version = 0;

    protected Map<String, String> properties = new HashMap<>();

    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public AbstractFileStore(String key, String name) {
        this.key = key;
        this.name = name;
    }

    public AbstractFileStore(FileStoreInfo fsInfo) {
        this.key = fsInfo.getFsKey();
        this.name = fsInfo.getFsName();
        this.enabled = fsInfo.getEnabled();
        this.comment = fsInfo.getComment();
        this.locations.addAll(fsInfo.getLocationsList());
        this.version = fsInfo.getVersion();
        this.properties = fsInfo.getPropertiesMap();
    }

    // Return file store key
    // Each file store has one unique key
    @Override
    public String key() {
        return key;
    }

    // Return file store name
    // Name is not ensured to be unique
    @Override
    public String name() {
        return name;
    }

    @Override
    public String getComment() {
        return comment;
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public boolean getEnabled() {
        return enabled;
    }

    @Override
    public boolean isBuiltin() {
        return builtin;
    }

    @Override
    public void setBuiltin(boolean builtin) {
        this.builtin = builtin;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public void increaseVersion() {
        this.version++;
    }

    // Return file store root path
    @Override
    public String rootPath() {
        assert (!locations.isEmpty());
        return locations.get(0);
    }

    // Serialize file store to protobuf format
    private FileStoreInfo toProtobufInternal(boolean includeSecret) {
        return toProtobufBuilder().build();
    }

    @Override
    public FileStoreInfo toProtobuf() {
        return toProtobufInternal(true /* includeSecret */);
    }

    @Override
    public FileStoreInfo toDebugProtobuf() {
        return toProtobufInternal(false /* includeSecret */);
    }

    protected FileStoreInfo.Builder toProtobufBuilder() {
        return FileStoreInfo.newBuilder()
                .setFsKey(key)
                .setFsName(name)
                .setEnabled(enabled)
                .setComment(comment)
                .addAllLocations(locations)
                .setVersion(version)
                .putAllProperties(properties);
    }
    /**
     * Allows update FileStore from @other if the corresponding field is set
     *
     * @param other the other FileStore instance to be merged
     */
    @Override
    public void mergeFrom(FileStore other) {
        if (!other.getComment().isEmpty()) {
            this.comment = other.getComment();
        }
        this.enabled = other.getEnabled();
        this.properties = other.getPropertiesMap();
    }

    @Override
    public boolean isPartitionedPrefixEnabled() {
        return false;
    }

    @Override
    public int numOfPartitionedPrefix() {
        return 0;
    }

    @Override
    public Map<String, String> getPropertiesMap() {
        return properties;
    }
    public static FileStore fromProtobuf(FileStoreInfo fsInfo) {
        switch (fsInfo.getFsType()) {
            case S3:
                return S3FileStore.fromProtobuf(fsInfo);
            case HDFS:
                return HDFSFileStore.fromProtobuf(fsInfo);
            case AZBLOB:
                return AzBlobFileStore.fromProtobuf(fsInfo);
            case ADLS2:
                return ADLS2FileStore.fromProtobuf(fsInfo);
            case GS:
                return GSFileStore.fromProtobuf(fsInfo);
            default:
                return InvalidFileStore.fromProtobuf(fsInfo);
        }
    }
}
