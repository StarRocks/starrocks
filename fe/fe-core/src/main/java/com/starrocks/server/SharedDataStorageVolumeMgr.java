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
import java.util.Set;
import java.util.stream.Collectors;

public class SharedDataStorageVolumeMgr extends StorageVolumeMgr {
    @Override
    public Long createStorageVolume(String name, String svType, List<String> locations, Map<String, String> params,
                                    Optional<Boolean> enabled, String comment)
            throws AlreadyExistsException, AnalysisException, DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (exists(name)) {
                throw new AlreadyExistsException(String.format("Storage volume '%s' already exists", name));
            }
            StorageVolume sv = new StorageVolume(0, name, svType, locations, params, enabled.orElse(true), comment);
            return Long.valueOf(GlobalStateMgr.getCurrentState().getStarOSAgent().addFileStore(sv.toFileStoreInfo()));
        }
    }

    @Override
    public void removeStorageVolume(String name) throws AnalysisException, DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume sv = getStorageVolumeByName(name);
            Preconditions.checkState(sv != null,
                    "Storage volume '%s' does not exist", name);
            Preconditions.checkState(defaultStorageVolumeId != sv.getId(),
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
                    Preconditions.checkState(sv.getId() != defaultStorageVolumeId, "Default volume can not be disabled");
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
    public StorageVolume getStorageVolume(long storageVolumeId) throws AnalysisException {
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
}
