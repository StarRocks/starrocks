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
import com.staros.credential.ADLS2Credential;
import com.staros.proto.ADLS2FileStoreInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.util.Constant;
import com.staros.util.LockCloseable;

import java.util.Arrays;
import java.util.List;

public class ADLS2FileStore extends AbstractFileStore {
    // https://xxx.dfs.core.windows.net
    private String endpoint;
    // credential
    private ADLS2Credential credential;

    @Override
    public FileStoreType type() {
        return FileStoreType.ADLS2;
    }

    @Override
    public boolean isValid() {
        if (endpoint.isEmpty()) {
            return false;
        }

        assert (!locations.isEmpty());
        String path = locations.get(0).substring(Constant.AZURE_ADLS2_PREFIX.length());
        if (path.isEmpty()) {
            return false;
        }

        return true;
    }

    public ADLS2FileStore(FileStoreInfo fsInfo, String endpoint, ADLS2Credential credential) {
        super(fsInfo);
        this.endpoint = endpoint;
        this.credential = credential;
    }

    public ADLS2FileStore(String key, String name, String endpoint, String path, ADLS2Credential credential) {
        super(key, name);
        this.endpoint = endpoint;
        this.credential = credential;
        locations.add(Constant.AZURE_ADLS2_PREFIX + path);
    }

    private FileStoreInfo toProtobufInternal(boolean includeSecret) {
        try (LockCloseable lockCloseable = new LockCloseable(lock.readLock())) {
            assert (!locations.isEmpty());

            ADLS2FileStoreInfo.Builder adls2Builder = ADLS2FileStoreInfo.newBuilder();
            adls2Builder.setEndpoint(endpoint);
            adls2Builder.setPath(locations.get(0).substring(Constant.AZURE_ADLS2_PREFIX.length()));
            if (includeSecret) {
                adls2Builder.setCredential(credential.toProtobuf());
            }

            return toProtobufBuilder()
                    .setFsType(FileStoreType.ADLS2)
                    .setAdls2FsInfo(adls2Builder)
                    .build();
        }
    }

    @Override
    public FileStoreInfo toProtobuf() {
        return toProtobufInternal(true /* includeSecret */);
    }

    @Override
    public FileStoreInfo toDebugProtobuf() {
        return toProtobufInternal(false /* includeSecret */);
    }

    @Override
    public void mergeFrom(FileStore fileStore) {
        Preconditions.checkArgument(fileStore instanceof ADLS2FileStore);
        super.mergeFrom(fileStore);
        ADLS2FileStore otherStore = (ADLS2FileStore) fileStore;
        if (!otherStore.endpoint.isEmpty()) {
            endpoint = otherStore.endpoint;
        }
        if (!otherStore.credential.isEmpty()) {
            credential = otherStore.credential;
        }
    }

    public static FileStore fromProtobuf(FileStoreInfo fsInfo) {
        ADLS2FileStoreInfo adls2Info = fsInfo.getAdls2FsInfo();
        String endpoint = adls2Info.getEndpoint();
        String path = adls2Info.getPath();
        if (fsInfo.getLocationsList().isEmpty()) {
            List<String> locations = Arrays.asList(Constant.AZURE_ADLS2_PREFIX + path);
            fsInfo = fsInfo.toBuilder().addAllLocations(locations).build();
        }
        ADLS2Credential credential = ADLS2Credential.fromProtobuf(adls2Info.getCredential());
        return new ADLS2FileStore(fsInfo, endpoint, credential);
    }
}
