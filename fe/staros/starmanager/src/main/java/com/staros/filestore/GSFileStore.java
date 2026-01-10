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
import com.staros.credential.GSCredential;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.GSFileStoreInfo;
import com.staros.util.Constant;
import com.staros.util.LockCloseable;

import java.util.Arrays;
import java.util.List;

public class GSFileStore extends AbstractFileStore {
    private String endpoint;
    private GSCredential credential;

    @Override
    public FileStoreType type() {
        return FileStoreType.GS;
    }

    @Override
    public boolean isValid() {
        assert (!locations.isEmpty());
        if (locations.isEmpty()) {
            return false;
        }
        String path = locations.get(0).substring(Constant.GS_PREFIX.length());
        if (path.isEmpty()) {
            return false;
        }
        return true;
    }

    public GSFileStore(FileStoreInfo fsInfo, String endpoint, GSCredential credential) {
        super(fsInfo);
        this.endpoint = endpoint;
        this.credential = credential;
    }

    public GSFileStore(String key, String name, String endpoint, String path, GSCredential credential) {
        super(key, name);
        this.endpoint = endpoint;
        this.credential = credential;
        locations.add(Constant.GS_PREFIX + path);
    }

    private FileStoreInfo toProtobufInternal(boolean includeSecret) {
        try (LockCloseable lockCloseable = new LockCloseable(lock.readLock())) {
            assert (!locations.isEmpty());

            GSFileStoreInfo.Builder gsBuilder = GSFileStoreInfo.newBuilder();
            gsBuilder.setEndpoint(endpoint);
            gsBuilder.setPath(locations.get(0).substring(Constant.GS_PREFIX.length()));
            if (includeSecret) {
                gsBuilder.setUseComputeEngineServiceAccount(credential.isUseComputeEngineServiceAccount());
                gsBuilder.setServiceAccountEmail(credential.getServiceAccountEmail());
                gsBuilder.setServiceAccountPrivateKeyId(credential.getServiceAccountPrivateKeyId());
                gsBuilder.setServiceAccountPrivateKey(credential.getServiceAccountPrivateKey());
                gsBuilder.setImpersonation(credential.getImpersonation());
            } else {
                gsBuilder.setUseComputeEngineServiceAccount(true);
            }

            return toProtobufBuilder()
                    .setFsType(FileStoreType.GS)
                    .setGsFsInfo(gsBuilder)
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
        Preconditions.checkArgument(fileStore instanceof GSFileStore);
        super.mergeFrom(fileStore);
        GSFileStore otherStore = (GSFileStore) fileStore;
        if (!otherStore.endpoint.isEmpty()) {
            endpoint = otherStore.endpoint;
        }
        if (!otherStore.credential.isEmpty()) {
            credential = otherStore.credential;
        }
    }

    public static FileStore fromProtobuf(FileStoreInfo fsInfo) {
        GSFileStoreInfo gsInfo = fsInfo.getGsFsInfo();
        String endpoint = gsInfo.getEndpoint();
        String path = gsInfo.getPath();
        if (fsInfo.getLocationsList().isEmpty()) {
            List<String> locations = Arrays.asList(Constant.GS_PREFIX + path);
            fsInfo = fsInfo.toBuilder().addAllLocations(locations).build();
        }
        return new GSFileStore(fsInfo, endpoint,
                new GSCredential(gsInfo.getUseComputeEngineServiceAccount(), gsInfo.getServiceAccountEmail(),
                        gsInfo.getServiceAccountPrivateKeyId(), gsInfo.getServiceAccountPrivateKey(), gsInfo.getImpersonation()));
    }
}
