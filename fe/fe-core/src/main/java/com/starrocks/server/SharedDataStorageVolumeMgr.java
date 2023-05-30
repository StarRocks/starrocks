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

package com.starrocks.server;

import com.staros.proto.FileStoreInfo;
import com.staros.util.LockCloseable;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.credential.CloudConfigurationConstants;
import com.starrocks.storagevolume.StorageVolume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SharedDataStorageVolumeMgr extends StorageVolumeMgr {
    public static final String BUILTIN_STORAGE_VOLUME = "builtin_storage_volume";

    @Override
    public StorageVolume getStorageVolumeByName(String svKey) throws AnalysisException {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            try {
                FileStoreInfo fileStoreInfo = GlobalStateMgr.getCurrentState().getStarOSAgent().getFileStoreByName(svKey);
                if (fileStoreInfo == null) {
                    return null;
                }
                return StorageVolume.fromFileStoreInfo(fileStoreInfo);
            } catch (DdlException e) {
                throw new AnalysisException(e.getMessage());
            }
        }
    }

    @Override
    public StorageVolume getStorageVolume(String storageVolumeId) throws AnalysisException {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            try {
                FileStoreInfo fileStoreInfo = GlobalStateMgr.getCurrentState().getStarOSAgent().getFileStore(storageVolumeId);
                if (fileStoreInfo == null) {
                    return null;
                }
                return StorageVolume.fromFileStoreInfo(fileStoreInfo);
            } catch (DdlException e) {
                throw new AnalysisException(e.getMessage());
            }
        }
    }

    @Override
    public List<String> listStorageVolumeNames() throws DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return GlobalStateMgr.getCurrentState().getStarOSAgent().listFileStore()
                    .stream().map(FileStoreInfo::getFsName).collect(Collectors.toList());
        }
    }

    @Override
    protected String createInternalNoLock(String name, String svType, List<String> locations,
                                          Map<String, String> params, Optional<Boolean> enabled, String comment)
            throws AnalysisException, DdlException {
        FileStoreInfo fileStoreInfo = StorageVolume.createFileStoreInfo(name, svType,
                locations, params, enabled.orElse(true), comment);
        return GlobalStateMgr.getCurrentState().getStarOSAgent().addFileStore(fileStoreInfo);
    }

    @Override
    protected void updateInternalNoLock(StorageVolume sv) throws DdlException {
        GlobalStateMgr.getCurrentState().getStarOSAgent().updateFileStore(sv.toFileStoreInfo());
    }

    @Override
    protected void removeInternalNoLock(StorageVolume sv) throws DdlException {
        GlobalStateMgr.getCurrentState().getStarOSAgent().removeFileStoreByName(sv.getName());
    }

    public void createOrUpdateBuiltinStorageVolume() throws DdlException, AnalysisException, AlreadyExistsException {
        if (Config.cloud_native_storage_type.isEmpty()) {
            return;
        }

        List<String> locations = parseLocationsFromConfig();
        Map<String, String> params = parseParamsFromConfig();

        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (exists(BUILTIN_STORAGE_VOLUME)) {
                updateStorageVolume(BUILTIN_STORAGE_VOLUME, params, Optional.empty(), "");
            } else {
                createStorageVolume(BUILTIN_STORAGE_VOLUME,
                        Config.cloud_native_storage_type, locations, params, Optional.of(true), "");
                if (getDefaultStorageVolumeId().isEmpty()) {
                    setDefaultStorageVolume(BUILTIN_STORAGE_VOLUME);
                }
            }
        }
    }

    private List<String> parseLocationsFromConfig() {
        List<String> locations = new ArrayList<>();
        switch (Config.cloud_native_storage_type.toLowerCase()) {
            case "s3":
                locations.add("s3://" + Config.aws_s3_path);
                break;
            case "hdfs":
                locations.add("hdfs://" + Config.cloud_native_hdfs_url);
                break;
            case "azblob":
                // TODO
            default:
                return locations;
        }
        return locations;
    }

    private Map<String, String> parseParamsFromConfig() {
        Map<String, String> params = new HashMap<>();
        switch (Config.cloud_native_storage_type.toLowerCase()) {
            case "s3":
                params.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, Config.aws_s3_access_key);
                params.put(CloudConfigurationConstants.AWS_S3_SECRET_KEY, Config.aws_s3_secret_key);
                params.put(CloudConfigurationConstants.AWS_S3_REGION, Config.aws_s3_region);
                params.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, Config.aws_s3_endpoint);
                params.put(CloudConfigurationConstants.AWS_S3_EXTERNAL_ID, Config.aws_s3_external_id);
                params.put(CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN, Config.aws_s3_iam_role_arn);
                break;
            case "hdfs":
                // TODO
            case "azblob":
                // TODO
            default:
                return params;
        }
        return params;
    }
}
