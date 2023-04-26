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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageVolumeMgr {
    private Map<String, StorageVolume> nameToSV = new HashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private String defaultSV;

    public void createStorageVolume(String name, String svType, List<String> locations, Map<String, String> params,
                                    Optional<Boolean> enabled, String comment)
            throws AlreadyExistsException, AnalysisException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (nameToSV.containsKey(name)) {
                throw new AlreadyExistsException(String.format("Storage Volume '%s' already exists", name));
            }
            StorageVolume sv = new StorageVolume(name, svType, locations, params, enabled.orElse(true), comment);
            nameToSV.put(name, sv);
        }
    }

    public void removeStorageVolume(String name) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToSV.containsKey(name),
                    "Storage Volume '%s' does not exist", name);
            Preconditions.checkState(!name.equals(defaultSV), "default storage volume can not be removed");
            nameToSV.remove(name);
        }
    }

    public void updateStorageVolume(String name, Map<String, String> params, Optional<Boolean> enabled, String comment,
                                    boolean asDefault) throws AnalysisException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToSV.containsKey(name),
                    "Storage Volume '%s' does not exist", name);

            StorageVolume sv = nameToSV.get(name);
            if (!params.isEmpty()) {
                sv.setStorageParams(params);
            }

            if (enabled.isPresent()) {
                if (!enabled.get()) {
                    Preconditions.checkState(!name.equals(defaultSV) && !asDefault,
                            "Default volume can not be disabled");
                }
                sv.setEnabled(enabled.get());
            }

            if (!comment.isEmpty()) {
                sv.setComment(comment);
            }

            if (asDefault) {
                setDefaultStorageVolume(name);
            }
        }
    }

    public void setDefaultStorageVolume(String svKey) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToSV.containsKey(svKey),
                    "Storage Volume '%s' does not exist", svKey);
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
}
