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
import com.starrocks.common.InvalidConfException;
import com.starrocks.persist.DropStorageVolumeLog;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.storagevolume.StorageVolume;

import java.io.IOException;
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
    public StorageVolume getStorageVolumeByName(String svName) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            for (StorageVolume sv : idToSV.values()) {
                if (sv.getName().equals(svName)) {
                    return sv;
                }
            }
        }
        return null;
    }

    @Override
    public StorageVolume getStorageVolume(String svId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return idToSV.get(svId);
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
                                          String comment) {
        String id = UUID.randomUUID().toString();
        StorageVolume sv = new StorageVolume(id, name, svType, locations, params, enabled.orElse(true), comment);
        GlobalStateMgr.getCurrentState().getEditLog().logCreateStorageVolume(sv);
        idToSV.put(id, sv);
        return id;
    }

    @Override
    protected void updateInternalNoLock(StorageVolume sv) {
        GlobalStateMgr.getCurrentState().getEditLog().logUpdateStorageVolume(sv);
        idToSV.put(sv.getId(), sv);
    }

    @Override
    protected void removeInternalNoLock(StorageVolume sv) {
        DropStorageVolumeLog log = new DropStorageVolumeLog(sv.getId());
        GlobalStateMgr.getCurrentState().getEditLog().logDropStorageVolume(log);
        idToSV.remove(sv.getId());
    }

    @Override
    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        SharedNothingStorageVolumeMgr data = (SharedNothingStorageVolumeMgr) reader.readJson(StorageVolumeMgr.class);
        this.storageVolumeToDbs = data.storageVolumeToDbs;
        this.storageVolumeToTables = data.storageVolumeToTables;
        this.defaultStorageVolumeId = data.defaultStorageVolumeId;
        this.idToSV = data.idToSV;
    }

    @Override
    public void replayCreateStorageVolume(StorageVolume sv) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            idToSV.put(sv.getId(), sv);
        }
    }

    @Override
    public void replayUpdateStorageVolume(StorageVolume sv) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            idToSV.put(sv.getId(), sv);
        }
    }

    @Override
    public void replayDropStorageVolume(DropStorageVolumeLog log) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            idToSV.remove(log.getId());
        }
    }

    @Override
    public boolean bindDbToStorageVolume(String svName, long dbId) {
        return true;
    }

    @Override
    public void replayBindDbToStorageVolume(String svId, long dbId) {

    }

    @Override
    public void unbindDbToStorageVolume(long dbId) {

    }

    @Override
    public boolean bindTableToStorageVolume(String svName, long dbId, long tableId) {
        return true;
    }

    @Override
    public void replayBindTableToStorageVolume(String svId, long tableId) {

    }

    @Override
    public void unbindTableToStorageVolume(long tableId) {

    }

    @Override
    public String createBuiltinStorageVolume() {
        return "";
    }

    @Override
    public void validateStorageVolumeConfig() throws InvalidConfException {

    }
}
