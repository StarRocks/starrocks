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

import com.google.gson.annotations.SerializedName;
import com.staros.util.LockCloseable;
import com.starrocks.common.AnalysisException;
import com.starrocks.storagevolume.StorageVolume;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class SharedNothingStorageVolumeMgr extends StorageVolumeMgr {
    @SerializedName("idToSV")
    private Map<String, StorageVolume> idToSV = new HashMap<>();

    @Override
    public StorageVolume getStorageVolumeByName(String svKey) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            for (StorageVolume sv : idToSV.values()) {
                if (sv.getName().equals(svKey)) {
                    return sv;
                }
            }
        }
        return null;
    }

    @Override
    public StorageVolume getStorageVolume(String storageVolumeId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return idToSV.get(storageVolumeId);
        }
    }

    @Override
    public List<String> listStorageVolumeNames() {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return idToSV.values().stream().map(StorageVolume::getName).collect(Collectors.toList());
        }
    }

    @Override
    protected String createInternalNoLock(String name, String svType, List<String> locations,
                                          Map<String, String> params, Optional<Boolean> enabled,
                                          String comment) throws AnalysisException {
        String id = UUID.randomUUID().toString();
        StorageVolume sv = new StorageVolume(id, name, svType, locations, params, enabled.orElse(true), comment);
        idToSV.put(id, sv);
        return id;
    }

    @Override
    protected void updateInternalNoLock(StorageVolume sv) {
        idToSV.put(sv.getId(), sv);
    }

    @Override
    protected void removeInternalNoLock(StorageVolume sv) {
        idToSV.remove(sv.getId(), sv);
    }
}
