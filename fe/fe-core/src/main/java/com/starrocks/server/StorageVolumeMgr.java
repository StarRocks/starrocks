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
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.DropStorageVolumeStmt;
import com.starrocks.sql.ast.SetDefaultStorageVolumeStmt;
import com.starrocks.storagevolume.StorageVolume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageVolumeMgr {
    private Map<String, StorageVolume> nameToSV = new HashMap<>();

    private Map<Long, StorageVolume> idToSV = new HashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private String defaultSV;

    private static final String ENABLED = "enabled";


    public void createStorageVolume(CreateStorageVolumeStmt stmt) throws AlreadyExistsException, AnalysisException {
        Map<String, String> params = new HashMap<>();
        Optional<Boolean> enabled = parseProperties(stmt.getProperties(), params);
        createStorageVolume(stmt.getName(), stmt.getStorageVolumeType(), stmt.getStorageLocations(), params,
                enabled, stmt.getComment());
    }

    public void createStorageVolume(String name, String svType, List<String> locations, Map<String, String> params,
                                    Optional<Boolean> enabled, String comment)
            throws AlreadyExistsException, AnalysisException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (nameToSV.containsKey(name)) {
                throw new AlreadyExistsException(String.format("Storage Volume '%s' already exists", name));
            }
            StorageVolume sv = new StorageVolume(name, svType, locations, params, enabled.orElse(true), comment);
            nameToSV.put(name, sv);
            idToSV.put(sv.getId(), sv);
        }
    }

    public void removeStorageVolume(DropStorageVolumeStmt stmt) {
        removeStorageVolume(stmt.getName());
    }

    public void removeStorageVolume(String name) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToSV.containsKey(name),
                    "Storage Volume '%s' does not exist", name);
            Preconditions.checkState(!name.equals(defaultSV), "default storage volume can not be removed");
            // TODO: check ref count
            StorageVolume sv = nameToSV.remove(name);
            idToSV.remove(sv.getId());
        }
    }

    public void updateStorageVolume(AlterStorageVolumeStmt stmt) throws AnalysisException {
        Map<String, String> params = new HashMap<>();
        Optional<Boolean> enabled = parseProperties(stmt.getProperties(), params);
        updateStorageVolume(stmt.getName(), params, enabled, stmt.getComment());
    }

    public void updateStorageVolume(String name, Map<String, String> params, Optional<Boolean> enabled, String comment)
            throws AnalysisException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToSV.containsKey(name),
                    "Storage Volume '%s' does not exist", name);

            StorageVolume sv = nameToSV.get(name);
            if (!params.isEmpty()) {
                sv.setCloudConfiguration(params);
            }

            if (enabled.isPresent()) {
                if (!enabled.get()) {
                    Preconditions.checkState(!name.equals(defaultSV), "Default volume can not be disabled");
                }
                sv.setEnabled(enabled.get());
            }

            if (!comment.isEmpty()) {
                sv.setComment(comment);
            }
        }
    }

    public void setDefaultStorageVolume(SetDefaultStorageVolumeStmt stmt) {
        setDefaultStorageVolume(stmt.getName());
    }

    public void setDefaultStorageVolume(String svKey) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToSV.containsKey(svKey),
                    "Storage Volume '%s' does not exist", svKey);
            StorageVolume sv = nameToSV.get(svKey);
            sv.setIsDefault();
            defaultSV = svKey;
        }
    }

    public String getDefaultSV() {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return defaultSV;
        }
    }

    public boolean exists(String svKey) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return nameToSV.containsKey(svKey);
        }
    }

    public StorageVolume getStorageVolume(String svKey) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return nameToSV.get(svKey);
        }
    }

    public List<String> listStorageVolumeNames() {
        return new ArrayList<>(nameToSV.keySet());
    }

    private Optional<Boolean> parseProperties(Map<String, String> properties, Map<String, String> params) {
        params.putAll(properties);
        Optional<Boolean> enabled = Optional.empty();
        if (params.containsKey(ENABLED)) {
            enabled = Optional.of(Boolean.parseBoolean(params.get(ENABLED)));
            params.remove(ENABLED);
        }
        return enabled;
    }
}
