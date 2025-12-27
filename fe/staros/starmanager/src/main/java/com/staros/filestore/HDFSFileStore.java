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
import com.staros.proto.HDFSFileStoreInfo;
import com.staros.util.LockCloseable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HDFSFileStore extends AbstractFileStore {
    private String userName = "";

    private Map<String, String> configuration = new HashMap<>();

    @Override
    public FileStoreType type() {
        return FileStoreType.HDFS;
    }

    @Override
    public String rootPath() {
        assert (!locations.isEmpty());

        // hdfs://<ip>/user/aaa/bbb
        String url = locations.get(0);
        if (!url.endsWith("/")) {
            return url + "/";
        }
        return url;
    }

    @Override
    public boolean isValid() {
        assert (!locations.isEmpty());

        String url = locations.get(0);
        if (url.isEmpty()) {
            return false;
        }

        return true;
    }

    public HDFSFileStore(FileStoreInfo fsInfo, String userName, Map<String, String> configuration) {
        super(fsInfo);
        this.configuration.putAll(configuration);
        this.userName = userName;
    }

    public HDFSFileStore(String key, String name, String url) {
        super(key, name);
        locations.add(url);
    }

    private FileStoreInfo toProtobufInternal(boolean includeSecret) {
        try (LockCloseable lockCloseable = new LockCloseable(lock.readLock())) {
            assert (!locations.isEmpty());

            HDFSFileStoreInfo.Builder hdfsBuilder = HDFSFileStoreInfo.newBuilder();
            hdfsBuilder.setUrl(locations.get(0));
            if (!userName.isEmpty()) {
                hdfsBuilder.setUsername(userName);
            }
            hdfsBuilder.putAllConfiguration(configuration);

            return toProtobufBuilder()
                    .setFsType(FileStoreType.HDFS)
                    .setHdfsFsInfo(hdfsBuilder)
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
        Preconditions.checkArgument(fileStore instanceof HDFSFileStore);
        super.mergeFrom(fileStore);
        HDFSFileStore otherStore = (HDFSFileStore) fileStore;
        if (!userName.equals(otherStore.userName)) {
            userName = otherStore.userName;
        }
        // add all k-v pairs into the configuration
        configuration.putAll(otherStore.configuration);
    }

    public static FileStore fromProtobuf(FileStoreInfo fsInfo) {
        HDFSFileStoreInfo hdfsInfo = fsInfo.getHdfsFsInfo();
        String url = hdfsInfo.getUrl();
        if (fsInfo.getLocationsList().isEmpty()) {
            List<String> locations = new ArrayList<>(Arrays.asList(url));
            fsInfo = fsInfo.toBuilder().addAllLocations(locations).build();
        }

        String userName = hdfsInfo.getUsername();
        Map<String, String> configuration = hdfsInfo.getConfigurationMap();

        return new HDFSFileStore(fsInfo, userName, configuration);
    }
}
