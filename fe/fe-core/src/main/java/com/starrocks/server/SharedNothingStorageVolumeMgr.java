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
import com.staros.util.LockCloseable;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.storagevolume.StorageVolume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SharedNothingStorageVolumeMgr extends StorageVolumeMgr {
    private Map<String, StorageVolume> nameToSV = new HashMap<>();

    @Override
    public Long createStorageVolume(String name, String svType, List<String> locations, Map<String, String> params,
                                    Optional<Boolean> enabled, String comment)
            throws AlreadyExistsException, AnalysisException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (nameToSV.containsKey(name)) {
                throw new AlreadyExistsException(String.format("Storage volume '%s' already exists", name));
            }
            long id = GlobalStateMgr.getCurrentState().getNextId();
            StorageVolume sv = new StorageVolume(id, name, svType, locations, params, enabled.orElse(true), comment);
            nameToSV.put(name, sv);
            return id;
        }
    }

    @Override
    public void removeStorageVolume(String name) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToSV.containsKey(name),
                    "Storage volume '%s' does not exist", name);
            StorageVolume sv = nameToSV.get(name);
            Preconditions.checkState(sv.getId() != defaultStorageVolumeId, "default storage volume can not be removed");
            Preconditions.checkState(!storageVolumeToDbs.containsKey(sv.getId())
                            && !storageVolumeToTables.containsKey(sv.getId()),
                    "Storage volume '%s' is referenced by db or table", name);
            nameToSV.remove(name);
        }
    }

    @Override
    public void updateStorageVolume(String name, Map<String, String> params, Optional<Boolean> enabled, String comment)
            throws AnalysisException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToSV.containsKey(name),
                    "Storage volume '%s' does not exist", name);

            StorageVolume sv = nameToSV.get(name);
            if (!params.isEmpty()) {
                sv.setCloudConfiguration(params);
            }

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
        }
    }

    @Override
    public void setDefaultStorageVolume(String svKey) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToSV.containsKey(svKey),
                    "Storage volume '%s' does not exist", svKey);
            StorageVolume sv = nameToSV.get(svKey);
            defaultStorageVolumeId = sv.getId();
        }
    }

    @Override
    public boolean exists(String svKey) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return nameToSV.containsKey(svKey);
        }
    }

    public StorageVolume getStorageVolumeByName(String svKey) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return nameToSV.get(svKey);
        }
    }

    @Override
    public StorageVolume getStorageVolume(long storageVolumeId) throws AnalysisException {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            for (StorageVolume sv : nameToSV.values()) {
                if (sv.getId() == storageVolumeId) {
                    return sv;
                }
            }
        }
        return null;
    }

    @Override
    public List<String> listStorageVolumeNames() {
        return new ArrayList<>(nameToSV.keySet());
    }
}
