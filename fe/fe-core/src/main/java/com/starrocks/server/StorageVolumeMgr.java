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

import com.staros.util.LockCloseable;
import com.starrocks.storagevolume.StorageVolume;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageVolumeMgr {
    private Map<String, StorageVolume> nameToSV = new HashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private String defaultSV;

    public void createStorageVolume(String name, String svType, String location, Map<String, String> params,
                                    Boolean enabled, String comment) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume sv = new StorageVolume(name, svType, location, params, enabled, comment);
            nameToSV.put(name, sv);
        }
    }

    public void removeStorageVolume(String name) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            nameToSV.remove(name);
        }
    }

    public void updateStorageVolume(String name, Map<String, String> params, Boolean enabled, String comment) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume sv = nameToSV.get(name);
            if (sv == null) {
                return;
            }

            if (!params.isEmpty()) {
                sv.setStorageParams(params);
            }

            if (enabled != null) {
                sv.setEnabled(enabled);
            }

            if (!comment.isEmpty()) {
                sv.setComment(comment);
            }
        }
    }

    public void setDefaultStorageVolume(String svKey) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            defaultSV = svKey;
        }
    }
}
