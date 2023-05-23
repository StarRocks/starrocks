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
import com.starrocks.common.DdlException;
import com.starrocks.storagevolume.StorageVolume;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SharedDataStorageVolumeMgr extends StorageVolumeMgr {
    @Override
    public Long createStorageVolume(String name, String svType, List<String> locations, Map<String, String> params,
                                    Optional<Boolean> enabled, String comment)
            throws AlreadyExistsException, AnalysisException, DdlException {
        if (exists(name)) {
            throw new AlreadyExistsException(String.format("Storage volume '%s' already exists", name));
        }
        StorageVolume sv = new StorageVolume(0, name, svType, locations, params, enabled.orElse(true), comment);
        return Long.valueOf(GlobalStateMgr.getCurrentState().getStarOSAgent().addFileStore(sv.toFileStoreInfo()));
    }

    @Override
    public void removeStorageVolume(String name) throws AnalysisException, DdlException {
        StorageVolume sv = getStorageVolume(name);
        Preconditions.checkState(sv != null,
                "Storage volume '%s' does not exist", name);
        Preconditions.checkState(!defaultSV.equals(name), "default storage volume can not be removed");
        Preconditions.checkState(!storageVolumeToDB.containsKey(sv.getId())
                        && !storageVolumeToTable.containsKey(sv.getId()),
                "Storage volume '%s' is referenced by db or table", name);
        GlobalStateMgr.getCurrentState().getStarOSAgent().removeFileStoreByName(name);
    }

    @Override
    public void updateStorageVolume(String name, Map<String, String> params,
                                    Optional<Boolean> enabled, String comment) throws DdlException, AnalysisException {
        StorageVolume sv = getStorageVolume(name);
        Preconditions.checkState(sv != null, "Storage volume '%s' does not exist", name);

        if (enabled.isPresent()) {
            boolean enabledValue = enabled.get();
            if (!enabledValue) {
                Preconditions.checkState(!sv.isDefault(), "Default volume can not be disabled");
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

    @Override
    public void setDefaultStorageVolume(String svKey) throws AnalysisException, DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume sv = getStorageVolume(svKey);
            Preconditions.checkState(sv != null, "Storage volume '%s' does not exist", svKey);
            sv.setIsDefault(true);
            GlobalStateMgr.getCurrentState().getStarOSAgent().updateFileStore(sv.toFileStoreInfo());
            if (!defaultSV.isEmpty()) {
                StorageVolume defaultStorageVolume = getStorageVolume(defaultSV);
                defaultStorageVolume.setIsDefault(false);
                GlobalStateMgr.getCurrentState().getStarOSAgent().updateFileStore(defaultStorageVolume.toFileStoreInfo());
            }
            this.defaultSV = svKey;
        }
    }

    @Override
    public boolean exists(String svKey) throws DdlException {
        try {
            StorageVolume sv = getStorageVolume(svKey);
            return sv != null;
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public StorageVolume getStorageVolume(String svKey) throws AnalysisException {
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

    @Override
    public List<String> listStorageVolumeNames() throws DdlException {
        return GlobalStateMgr.getCurrentState().getStarOSAgent().listFileStore()
                .stream().map(FileStoreInfo::getFsName).collect(Collectors.toList());
    }
}
