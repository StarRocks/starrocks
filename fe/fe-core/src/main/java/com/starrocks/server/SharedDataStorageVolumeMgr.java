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

import com.google.common.base.Preconditions;
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
import java.util.Set;
import java.util.stream.Collectors;

public class SharedDataStorageVolumeMgr extends StorageVolumeMgr {
    public static final String BUILTIN_STORAGE_VOLUME = "builtin_storage_volume";

    public String createStorageVolume(String name, String svType, List<String> locations, Map<String, String> params,
                                      Optional<Boolean> enabled, String comment)
            throws AlreadyExistsException, AnalysisException, DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (exists(name)) {
                throw new AlreadyExistsException(String.format("Storage volume '%s' already exists", name));
            }
            StorageVolume sv = new StorageVolume("", name, svType, locations, params, enabled.orElse(true), comment);
            return GlobalStateMgr.getCurrentState().getStarOSAgent().addFileStore(sv.toFileStoreInfo());
        }
    }

    @Override
    public void removeStorageVolume(String name) throws AnalysisException, DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume sv = getStorageVolumeByName(name);
            Preconditions.checkState(sv != null,
                    "Storage volume '%s' does not exist", name);
            Preconditions.checkState(!defaultStorageVolumeId.equals(sv.getId()),
                    "default storage volume can not be removed");
            Set<Long> dbs = storageVolumeToDbs.get(sv.getId());
            Set<Long> tables = storageVolumeToTables.get(sv.getId());
            Preconditions.checkState(dbs == null && tables == null,
                    "Storage volume '%s' is referenced by dbs or tables, dbs: %s, tables: %s",
                    name, dbs != null ? dbs.toString() : "[]", tables != null ? tables.toString() : "[]");
            GlobalStateMgr.getCurrentState().getStarOSAgent().removeFileStoreByName(name);
        }
    }

    @Override
    public void updateStorageVolume(String name, Map<String, String> params,
                                    Optional<Boolean> enabled, String comment) throws DdlException, AnalysisException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume sv = getStorageVolumeByName(name);
            Preconditions.checkState(sv != null, "Storage volume '%s' does not exist", name);

            if (enabled.isPresent()) {
                boolean enabledValue = enabled.get();
                if (!enabledValue) {
                    Preconditions.checkState(!sv.getId().equals(defaultStorageVolumeId),
                            "Default volume can not be disabled");
                }
                sv.setEnabled(enabledValue);
            }

            if (!comment.isEmpty()) {
                sv.setComment(comment);
            }

            if (!params.isEmpty()) {
                sv.setCloudConfiguration(params);
            }

            GlobalStateMgr.getCurrentState().getStarOSAgent().updateFileStore(sv.toFileStoreInfo());
        }
    }

    @Override
    public void setDefaultStorageVolume(String svKey) throws AnalysisException, DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume sv = getStorageVolumeByName(svKey);
            Preconditions.checkState(sv != null, "Storage volume '%s' does not exist", svKey);
            Preconditions.checkState(sv.getEnabled(), "Storage volume '%s' is disabled", svKey);
            this.defaultStorageVolumeId = sv.getId();
        }
    }

    @Override
    public boolean exists(String svKey) throws DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            try {
                StorageVolume sv = getStorageVolumeByName(svKey);
                return sv != null;
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
        }
    }

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
        // TODO: should be supported by staros. We use id for persistence, storage volume needs to be get by id.
        return null;
    }

    @Override
    public List<String> listStorageVolumeNames() throws DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return GlobalStateMgr.getCurrentState().getStarOSAgent().listFileStore()
                    .stream().map(FileStoreInfo::getFsName).collect(Collectors.toList());
        }
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
