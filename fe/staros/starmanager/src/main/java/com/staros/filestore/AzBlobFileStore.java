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
import com.staros.credential.AzBlobCredential;
import com.staros.proto.AzBlobFileStoreInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.util.Constant;
import com.staros.util.LockCloseable;

import java.util.Arrays;
import java.util.List;

public class AzBlobFileStore extends AbstractFileStore {
    // https://xxx.blob.core.windows.net
    private String endpoint;
    // credential
    private AzBlobCredential credential;

    @Override
    public FileStoreType type() {
        return FileStoreType.AZBLOB;
    }

    @Override
    public boolean isValid() {
        if (endpoint.isEmpty()) {
            return false;
        }

        assert (!locations.isEmpty());
        String path = locations.get(0).substring(Constant.AZURE_BLOB_PREFIX.length());
        if (path.isEmpty()) {
            return false;
        }

        return true;
    }

    public AzBlobFileStore(FileStoreInfo fsInfo, String endpoint, AzBlobCredential credential) {
        super(fsInfo);
        this.endpoint = endpoint;
        this.credential = credential;
    }

    public AzBlobFileStore(String key, String name, String endpoint, String path, AzBlobCredential credential) {
        super(key, name);
        this.endpoint = endpoint;
        this.credential = credential;
        locations.add(Constant.AZURE_BLOB_PREFIX + path);
    }

    private FileStoreInfo toProtobufInternal(boolean includeSecret) {
        try (LockCloseable lockCloseable = new LockCloseable(lock.readLock())) {
            assert (!locations.isEmpty());

            AzBlobFileStoreInfo.Builder azblobBuilder = AzBlobFileStoreInfo.newBuilder();
            azblobBuilder.setEndpoint(endpoint);
            azblobBuilder.setPath(locations.get(0).substring(Constant.AZURE_BLOB_PREFIX.length()));
            if (includeSecret) {
                azblobBuilder.setCredential(credential.toProtobuf());
            }

            return toProtobufBuilder()
                    .setFsType(FileStoreType.AZBLOB)
                    .setAzblobFsInfo(azblobBuilder)
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
        Preconditions.checkArgument(fileStore instanceof AzBlobFileStore);
        super.mergeFrom(fileStore);
        AzBlobFileStore otherStore = (AzBlobFileStore) fileStore;
        if (!otherStore.endpoint.isEmpty()) {
            endpoint = otherStore.endpoint;
        }
        if (!otherStore.credential.isEmpty()) {
            credential = otherStore.credential;
        }
    }

    public static FileStore fromProtobuf(FileStoreInfo fsInfo) {
        AzBlobFileStoreInfo azblobInfo = fsInfo.getAzblobFsInfo();
        String endpoint = azblobInfo.getEndpoint();
        String path = azblobInfo.getPath();
        if (fsInfo.getLocationsList().isEmpty()) {
            List<String> locations = Arrays.asList(Constant.AZURE_BLOB_PREFIX + path);
            fsInfo = fsInfo.toBuilder().addAllLocations(locations).build();
        }
        AzBlobCredential credential = AzBlobCredential.fromProtobuf(azblobInfo.getCredential());
        return new AzBlobFileStore(fsInfo, endpoint, credential);
    }
}
