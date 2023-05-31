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
import com.google.gson.annotations.SerializedName;
import com.staros.util.LockCloseable;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.storagevolume.StorageVolume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class SharedNothingStorageVolumeMgr extends StorageVolumeMgr {
    @SerializedName("nameToSV")
    private Map<String, StorageVolume> nameToSV = new HashMap<>();

    @Override
    public String createStorageVolume(String name, String svType, List<String> locations, Map<String, String> params,
                                    Optional<Boolean> enabled, String comment)
            throws AlreadyExistsException, AnalysisException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (nameToSV.containsKey(name)) {
                throw new AlreadyExistsException(String.format("Storage volume '%s' already exists", name));
            }
            String id = UUID.randomUUID().toString();
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
            Preconditions.checkState(!sv.getId().equals(defaultStorageVolumeId),
                    "default storage volume can not be removed");
            Set<Long> dbs = storageVolumeToDbs.get(sv.getId());
            Set<Long> tables = storageVolumeToTables.get(sv.getId());
            Preconditions.checkState(dbs == null && tables == null,
                    "Storage volume '%s' is referenced by dbs or tables, dbs: %s, tables: %s",
                    name, dbs != null ? dbs.toString() : "[]", tables != null ? tables.toString() : "[]");
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
            StorageVolume copied = new StorageVolume(sv);
            if (!params.isEmpty()) {
                copied.setCloudConfiguration(params);
            }

            if (enabled.isPresent()) {
                boolean enabledValue = enabled.get();
                if (!enabledValue) {
                    Preconditions.checkState(!copied.getId().equals(defaultStorageVolumeId),
                            "Default volume can not be disabled");
                }
                copied.setEnabled(enabledValue);
            }

            if (!comment.isEmpty()) {
                copied.setComment(comment);
            }

            nameToSV.put(name, copied);
        }
    }

    @Override
    public void setDefaultStorageVolume(String svKey) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToSV.containsKey(svKey),
                    "Storage volume '%s' does not exist", svKey);
            StorageVolume sv = nameToSV.get(svKey);
            Preconditions.checkState(sv.getEnabled(), "Storage volume '%s' is disabled", svKey);
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
    public StorageVolume getStorageVolume(String storageVolumeId) throws AnalysisException {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            for (StorageVolume sv : nameToSV.values()) {
                if (sv.getId().equals(storageVolumeId)) {
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
